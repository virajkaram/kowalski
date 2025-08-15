import datetime
import fire
import multiprocessing
import numpy as np
import os
import pathlib
import pytz
import time
from tqdm import tqdm
import traceback
from typing import Sequence
from typing import Mapping
from copy import deepcopy
import io
import fastavro


from kowalski.utils import (
    deg2dms,
    deg2hms,
    great_circle_distance,
    in_ellipse,
    radec2lb,
    retry,
    timer,
    init_db_sync,
    Mongo,
)

from kowalski.config import load_config
from kowalski.log import log


""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]
init_db_sync(config=config)

collection_alerts: str = config["database"]["collections"]["alerts_wtp"]
collection_alerts_aux: str = config["database"]["collections"]["alerts_wtp_aux"]
cross_match_config: dict = config["database"]["xmatch"]["WTP"]
verbose = 1


def utc_now():
    return datetime.datetime.now(pytz.utc)


def get_mongo_client() -> Mongo:
    n_retries = 0
    while n_retries < 10:
        try:
            mongo = Mongo(
                host=config["database"]["host"],
                port=config["database"]["port"],
                replica_set=config["database"]["replica_set"],
                username=config["database"]["username"],
                password=config["database"]["password"],
                db=config["database"]["db"],
                srv=config["database"]["srv"],
                verbose=0,
            )
        except Exception as e:
            traceback.print_exc()
            log(e)
            log("Failed to connect to the database, waiting 15 seconds before retry")
            time.sleep(15)
            continue
        return mongo
    raise Exception("Failed to connect to the database after 10 retries")


def alert_mongify(alert: Mapping, date_key: str = "mjd") -> Mapping:
    """
    Prepare a raw alert for ingestion into MongoDB:
        - add a placeholder for ML-based classifications
        - add coordinates for 2D spherical indexing and compute Galactic coordinates
        - extract the prv_candidates section
        - extract the fp_hists section (if it exists)

    :param alert:
    :return:
    """

    doc = dict(alert)

    # let mongo create a unique _id

    # placeholders for classifications
    doc["classifications"] = dict()

    # GeoJSON for 2D indexing
    doc["coordinates"] = {}
    _ra = doc["candidate"]["ra"]
    _dec = doc["candidate"]["dec"]
    # string format: H:M:S, D:M:S
    _radec_str = [deg2hms(_ra), deg2dms(_dec)]
    doc["coordinates"]["radec_str"] = _radec_str
    # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
    _radec_geojson = [_ra - 180.0, _dec]
    doc["coordinates"]["radec_geojson"] = {
        "type": "Point",
        "coordinates": _radec_geojson,
    }

    # Galactic coordinates l and b
    l, b = radec2lb(doc["candidate"]["ra"], doc["candidate"]["dec"])
    doc["coordinates"]["l"] = l
    doc["coordinates"]["b"] = b

    prv_candidates = deepcopy(doc["prv_candidates"])
    doc.pop("prv_candidates", None)
    if prv_candidates is None:
        prv_candidates = []

    # extract the fp_hists section, if it exists
    fp_hists = deepcopy(doc.get("fp_hists", None))
    doc.pop("fp_hists", None)
    if fp_hists is None:
        fp_hists = []
    else:
        # sort by date
        fp_hists = sorted(fp_hists, key=lambda k: k[date_key])

    return doc, prv_candidates, fp_hists


def process_file(argument_list: Sequence):
    def read_schema_data(bytes_io):
        """Read data that already has an Avro schema.

        :param bytes_io: `_io.BytesIO` Data to be decoded.
        :return: `dict` Decoded data.
        """
        bytes_io.seek(0)
        message = fastavro.reader(bytes_io)
        return message

    def decode_message(file_name):
        """
        Decode Avro message according to a schema.

        :param msg: The Kafka message result from consumer.poll()
        :return:
        """

        # open the file
        with open(file_name, "rb") as f:
            message = f.read()
        try:
            bytes_io = io.BytesIO(message)
            decoded_msg = read_schema_data(bytes_io)
        except AssertionError:
            decoded_msg = None
        finally:
            return decoded_msg

    def alert_filter__xmatch(alert: Mapping, cross_match_config: dict) -> dict:
        """Cross-match alerts against external catalogs"""

        xmatches = dict()

        try:
            ra = float(alert["candidate"]["ra"])
            dec = float(alert["candidate"]["dec"])
            ra_geojson = float(alert["candidate"]["ra"])
            # geojson-friendly ra:
            ra_geojson -= 180.0
            dec_geojson = float(alert["candidate"]["dec"])

            """ catalogs """
            matches = []
            for catalog in cross_match_config:
                try:
                    # if the catalog has "distance", "ra", "dec" in its config, then it is a catalog with distance
                    if cross_match_config[catalog].get("use_distance", False):
                        matches = alert_filter__xmatch_distance(
                            ra,
                            dec,
                            ra_geojson,
                            dec_geojson,
                            catalog,
                            cross_match_config,
                        )
                    else:
                        matches = alert_filter__xmatch_no_distance(
                            ra_geojson, dec_geojson, catalog, cross_match_config
                        )
                except Exception as e:
                    log(f"Failed to cross-match {catalog}: {str(e)}")
                    matches = []
                xmatches[catalog] = matches

            # clean up after thyself
            del ra, dec, ra_geojson, dec_geojson, matches, cross_match_config

        except Exception as e:
            log(f"Failed catalogs cross-match: {str(e)}")

        return xmatches

    def alert_filter__xmatch_no_distance(
        ra_geojson: float,
        dec_geojson: float,
        catalog: str,
        cross_match_config: dict,
    ) -> dict:
        """Cross-match alerts against external catalogs"""

        matches = []

        try:
            # cone search radius:
            catalog_cone_search_radius = float(
                cross_match_config[catalog]["cone_search_radius"]
            )
            # convert to rad:
            if cross_match_config[catalog]["cone_search_unit"] == "arcsec":
                catalog_cone_search_radius *= np.pi / 180.0 / 3600.0
            elif cross_match_config[catalog]["cone_search_unit"] == "arcmin":
                catalog_cone_search_radius *= np.pi / 180.0 / 60.0
            elif cross_match_config[catalog]["cone_search_unit"] == "deg":
                catalog_cone_search_radius *= np.pi / 180.0
            elif cross_match_config[catalog]["cone_search_unit"] == "rad":
                pass
            else:
                raise Exception(
                    f"Unknown cone search radius units for {catalog}."
                    " Must be one of [deg, rad, arcsec, arcmin]"
                )

            catalog_filter = cross_match_config[catalog]["filter"]
            catalog_projection = cross_match_config[catalog]["projection"]

            object_position_query = dict()
            object_position_query["coordinates.radec_geojson"] = {
                "$geoWithin": {
                    "$centerSphere": [
                        [ra_geojson, dec_geojson],
                        catalog_cone_search_radius,
                    ]
                }
            }
            s = retry(mongo.db[catalog].find)(
                {**object_position_query, **catalog_filter}, {**catalog_projection}
            )
            matches = list(s)

        except Exception as e:
            log(str(e))

        return matches

    def alert_filter__xmatch_distance(
        ra: float,
        dec: float,
        ra_geojson: float,
        dec_geojson: float,
        catalog: str,
        cross_match_config: dict,
    ) -> dict:
        """
        Run cross-match with catalogs that have a distance value

        :param alert:
        :param catalog: name of the catalog (collection) to cross-match with
        :return:
        """

        matches = []

        try:
            catalog_cm_at_distance = cross_match_config[catalog]["cm_at_distance"]
            catalog_cm_low_distance = cross_match_config[catalog]["cm_low_distance"]
            # cone search radius:
            catalog_cone_search_radius = float(
                cross_match_config[catalog]["cone_search_radius"]
            )
            # convert to rad:
            if cross_match_config[catalog]["cone_search_unit"] == "arcsec":
                catalog_cone_search_radius *= np.pi / 180.0 / 3600.0
            elif cross_match_config[catalog]["cone_search_unit"] == "arcmin":
                catalog_cone_search_radius *= np.pi / 180.0 / 60.0
            elif cross_match_config[catalog]["cone_search_unit"] == "deg":
                catalog_cone_search_radius *= np.pi / 180.0
            elif cross_match_config[catalog]["cone_search_unit"] == "rad":
                pass

            catalog_filter = cross_match_config[catalog]["filter"]
            catalog_projection = cross_match_config[catalog]["projection"]

            # first do a coarse search of everything that is around
            object_position_query = dict()
            object_position_query["coordinates.radec_geojson"] = {
                "$geoWithin": {
                    "$centerSphere": [
                        [ra_geojson, dec_geojson],
                        catalog_cone_search_radius,
                    ]
                }
            }
            galaxies = list(
                retry(mongo.db[catalog].find)(
                    {**object_position_query, **catalog_filter}, {**catalog_projection}
                )
            )

            distance_value = cross_match_config[catalog]["distance_value"]
            distance_method = cross_match_config[catalog]["distance_method"]

            # these guys are very big, so check them separately
            M31 = {
                "_id": 596900,
                "name": "PGC2557",
                "ra": 10.6847,
                "dec": 41.26901,
                "a": 6.35156,
                "b2a": 0.32,
                "pa": 35.0,
                "z": -0.00100100006,
                "DistMpc": 0.778,
                "sfr_fuv": None,
                "mstar": 253816876.412914,
                "sfr_ha": 0,
                "coordinates": {"radec_str": ["00:42:44.3503", "41:16:08.634"]},
            }
            M33 = {
                "_id": 597543,
                "name": "PGC5818",
                "ra": 23.46204,
                "dec": 30.66022,
                "a": 2.35983,
                "b2a": 0.59,
                "pa": 23.0,
                "z": -0.000597000006,
                "DistMpc": 0.869,
                "sfr_fuv": None,
                "mstar": 4502777.420493,
                "sfr_ha": 0,
                "coordinates": {"radec_str": ["01:33:50.8900", "30:39:36.800"]},
            }

            if distance_value == "z" or distance_method in ["redshift", "z"]:
                M31[distance_value] = M31["z"]
                M33[distance_value] = M33["z"]
            else:
                M31[distance_value] = M31["DistMpc"]
                M33[distance_value] = M33["DistMpc"]

            for galaxy in galaxies + [M31, M33]:
                try:
                    alpha1, delta01 = galaxy["ra"], galaxy["dec"]

                    redshift, distmpc = None, None
                    if distance_value == "z" or distance_method in [
                        "redshift",
                        "z",
                    ]:
                        redshift = galaxy[distance_value]

                        if redshift < 0.01:
                            # for nearby galaxies and galaxies with negative redshifts, do a `catalog_cm_low_distance` arc-minute cross-match
                            # (cross-match radius would otherwise get un-physically large for nearby galaxies)
                            cm_radius = catalog_cm_low_distance / 3600
                        else:
                            # For distant galaxies, set the cross-match radius to 30 kpc at the redshift of the host galaxy
                            cm_radius = (
                                catalog_cm_at_distance * (0.05 / redshift) / 3600
                            )
                    else:
                        distmpc = galaxy[distance_value]

                        if distmpc < 40:
                            # for nearby galaxies, do a `catalog_cm_low_distance` arc-minute cross-match
                            cm_radius = catalog_cm_low_distance / 3600
                        else:
                            # For distant galaxies, set the cross-match radius to 30 kpc at the distance (in Mpc) of the host galaxy
                            cm_radius = np.rad2deg(
                                np.arctan(catalog_cm_at_distance / (distmpc * 10**3))
                            )

                    in_galaxy = in_ellipse(ra, dec, alpha1, delta01, cm_radius, 1, 0)

                    if in_galaxy:
                        match = galaxy
                        distance_arcsec = round(
                            great_circle_distance(ra, dec, alpha1, delta01) * 3600,
                            2,
                        )
                        # also add a physical distance parameter for redshifts in the Hubble flow
                        if redshift is not None and redshift > 0.005:
                            distance_kpc = round(
                                great_circle_distance(ra, dec, alpha1, delta01)
                                * 3600
                                * (redshift / 0.05),
                                2,
                            )
                        elif distmpc is not None and distmpc > 0.005:
                            distance_kpc = round(
                                np.deg2rad(
                                    great_circle_distance(ra, dec, alpha1, delta01)
                                )
                                * distmpc
                                * 10**3,
                                2,
                            )
                        else:
                            distance_kpc = -1

                        match["coordinates"]["distance_arcsec"] = distance_arcsec
                        match["coordinates"]["distance_kpc"] = distance_kpc
                        matches.append(match)
                except Exception as e:
                    log(f"Could not crossmatch with galaxy {str(galaxy)} : {str(e)}")

            return matches

        except Exception as e:
            log(f"Could not crossmatch with ANY galaxies: {str(e)}")

        return matches


    def format_fp_hists(alert, fp_hists):
        if len(fp_hists) == 0:
            return []
        # sort by mjd
        fp_hists = sorted(fp_hists, key=lambda x: x["mjd"])

        # deduplicate by jd. We noticed in production that sometimes there are
        # multiple fp_hist entries with the same jd, which is not supposed to happen
        # and can affect our concurrency avoidance logic in update_fp_hists and take more space
        # This breaks the WTP ingestion, because W1 and W2 obs have the same mjd.
        # fp_hists = [
        #     fp_hist
        #     for i, fp_hist in enumerate(fp_hists)
        #     if i == 0 or fp_hist["mjd"] != fp_hists[i - 1]["mjd"]
        # ]

        # add the "alert_mag" field to the new fp_hist
        # as well as alert_ra, alert_dec
        for i, fp in enumerate(fp_hists):
            snr = fp.get("forcediffimflux", np.nan) / fp.get("forcediffimfluxunc", np.nan)
            fp_hists[i] = {
                **fp,
                "mag": fp.get("forcediffmagpsf", np.nan),
                "magerr": fp.get("forcediffsigmapsf", np.nan),
                "snr": snr,
                "limmag3sig": fp.get("diffmaglim", np.nan) - 2.5*np.log10(3.0 / 5.0),
                "limmag5sig": fp.get("diffmaglim", np.nan),
                "alert_mag": alert["candidate"]["magpsf"],
                "alert_ra": alert["candidate"]["ra"],
                "alert_dec": alert["candidate"]["dec"],
            }

        return fp_hists

    def update_fp_hists(alert, formatted_fp_hists):
        # update the existing fp_hist with the new one
        # instead of treating it as a set,
        # if some entries have the same jd, keep the one with the highest alert_mag

        # if we have no fp_hists to add, we don't do anything
        if len(formatted_fp_hists) == 0:
            return

        with timer(
            f"Updating fp_hists of {alert['objectId']} {alert['candid']}",
            verbose > 1,
        ):
            # pipeline that returns the very last fp_hists entry from the DB
            last_fp_hist_pipeline = [
                # 0. match the document and check that the fp_hists field exists
                {"$match": {"_id": alert["objectId"], "fp_hists": {"$exists": True}}},
                # 2. only keep the last fp_hists entry and call it fp_hist
                {
                    "$project": {
                        "fp_hist": {"$arrayElemAt": ["$fp_hists", -1]},
                    }
                },
                # 3. project only the jd and alert_mag, alert_ra, alert_dec fields in the fp_hists, as well as the n_fp_hists
                {
                    "$project": {
                        "fp_hist": {
                            "jd": "$fp_hist.jd",
                            "alert_mag": "$fp_hist.alert_mag",
                            "alert_ra": "$fp_hist.alert_ra",
                            "alert_dec": "$fp_hist.alert_dec",
                        },
                    }
                },
            ]

            # get the very last fp_hists entry from the DB
            last_fp_hist = (
                mongo.db[collection_alerts_aux]
                .aggregate(last_fp_hist_pipeline, allowDiskUse=True)
                .next()
            )

            if len(last_fp_hist["fp_hist"]) is None:
                replace_entry = True
            else:
                last_alert_mag = last_fp_hist["fp_hist"].get("alert_mag")
                current_alert_mag = alert["candidate"].get("magpsf")
                replace_entry = (current_alert_mag < last_alert_mag)

            if replace_entry:
                # replace the fp_hists entry
                mongo.db[collection_alerts_aux].update_one(
                    {
                        "_id": alert["objectId"],
                    },
                    {
                        "$set": {
                            "fp_hists": formatted_fp_hists
                        }
                    },
                )
                return formatted_fp_hists

            else:
                return {}

            # # pipeline that updates the fp_hists array if necessary
            # update_pipeline = [
            #     # 0. match the document
            #     {"$match": {"_id": alert["objectId"]}},
            #     # 1. concat the new fp_hists with the existing ones
            #     {
            #         "$project": {
            #             "all_fp_hists": {
            #                 "$concatArrays": [
            #                     {"$ifNull": ["$fp_hists", []]},
            #                     formatted_fp_hists,
            #                 ]
            #             }
            #         }
            #     },
            #     # 2. unwind the resulting array to get one document per fp_hist
            #     {"$unwind": "$all_fp_hists"},
            #     # 3. group by mjd and keep the one with the highest alert_mag for each mjd
            #     {
            #         "$set": {
            #             "all_fp_hists.alert_mag": {
            #                 "$cond": {
            #                     "if": {
            #                         "$eq": [
            #                             {"$type": "$all_fp_hists.alert_mag"},
            #                             "missing",
            #                         ]
            #                     },
            #                     "then": -99999.0,
            #                     "else": "$all_fp_hists.alert_mag",
            #                 }
            #             }
            #         }
            #     },
            #     # 4. sort by mjd and alert_mag
            #     {
            #         "$sort": {
            #             "all_fp_hists.mjd": 1,
            #             "all_fp_hists.alert_mag": 1,
            #         }
            #     },
            #     # 5. group all the deduplicated fp_hists back into an array, keeping the first one (the brightest at each mjd)
            #     {
            #         "$group": {
            #             "_id": "$all_fp_hists.mjd",
            #             "fp_hist": {"$first": "$$ROOT.all_fp_hists"},
            #         }
            #     },
            #     # 6. sort by mjd again
            #     {"$sort": {"fp_hist.mjd": 1}},
            #     # 7. group all the fp_hists documents back into a single array
            #     {"$group": {"_id": None, "fp_hists": {"$push": "$fp_hist"}}},
            #     # 8. project only the new fp_hists array
            #     {"$project": {"fp_hists": 1, "_id": 0}},
            # ]
            #
            # n_retries = 0
            # while True:
            #     try:
            #         # run the update pipeline
            #         new_fp_hists = (
            #             mongo.db[collection_alerts_aux]
            #             .aggregate(
            #                 update_pipeline,
            #                 allowDiskUse=True,
            #             )
            #             .next()
            #             .get("fp_hists", [])
            #         )
            #
            #         # we apply some conditions when running find_one_and_update to avoid concurrency
            #         # issues where another process might have updated the fp_hists while we were
            #         # calculating our updated fp_hists
            #
            #         update_conditions = {
            #             "_id": alert["objectId"],
            #         }
            #
            #         result = mongo.db[
            #             collection_alerts_aux
            #         ].find_one_and_update(
            #             update_conditions,
            #             {"$set": {"fp_hists": new_fp_hists}},
            #         )
            #     except Exception as e:
            #         log(
            #             f"Error occured trying to update fp_hists of {alert['objectId']} {alert['candid']}: {str(e)}"
            #         )
            #         result = None
            #     if (
            #         result is None
            #     ):  # conditions not met, likely to be a concurrency issue, retry
            #         n_retries += 1
            #         if n_retries > 10:
            #             log(
            #                 f"Failed to update fp_hists of {alert['objectId']} {alert['candid']}"
            #             )
            #             break
            #         else:
            #             log(
            #                 f"Retrying to update fp_hists of {alert['objectId']} {alert['candid']}"
            #             )
            #             # add a random sleep between 0 and 5s, this should help avoid multiple processes from retrying at the exact same time
            #             time.sleep(np.random.uniform(0, 5))
            #     else:
            #         break
            #
            # # query the DB for the last 30 days of fp_hists to get the updated fp_hists
            # new_fp_hists = list(
            #     mongo.db[collection_alerts_aux]
            #     .find(
            #         {
            #             "_id": alert["objectId"],
            #         },
            #         {"fp_hists": 1},
            #     )
            #     .sort([("mjd", 1)])
            # )
            # if len(new_fp_hists) > 0:
            #     new_fp_hists = new_fp_hists[0]["fp_hists"]
            # else:
            #     new_fp_hists = []


    def process_alert(alert: Mapping, topic: str, cross_match_config: dict):
        """Alert brokering task run by dask.distributed workers

        :param avro_msg: avro message from Kafka stream
        :param topic: Kafka stream topic name for bookkeeping
        :return:
        """

        # get worker running current task
        # worker = dask.distributed.get_worker()
        # alert_worker = worker.plugins["worker-init"].alert_worker
        #
        # with timer("Decoding alert", alert_worker.verbose > 1):
        #     msg_decoded = alert_worker.decode_message(avro_msg)

        candid = alert["candid"]
        object_id = alert["objectId"]
        if (
                retry(mongo.db[collection_alerts].count_documents)(
                    {"candid": candid}, limit=1
                )
                == 1
        ):
            # this alert has already been processed, skip it
            log(f"Alert {object_id} {candid} already processed, skipping")
            return

        alert["fp_hists"] = alert.pop("fp_records")
        # candid not in db, ingest decoded avro packet into db
        with timer(f"Mongification of {object_id} {candid}"):
            alert, prv_candidates, fp_hists = alert_mongify(alert, date_key="mjd")

        # future: add ML model filtering here

        with timer(f"Ingesting {object_id} {candid}", verbose > 1):
            mongo.insert_one(
                collection=collection_alerts, document=alert
            )

        # prv_candidates: pop nulls - save space
        prv_candidates = [
            {kk: vv for kk, vv in prv_candidate.items() if vv is not None}
            for prv_candidate in prv_candidates
        ]

        # fp_hists: pop nulls - save space
        fp_hists = [
            {
                kk: vv
                for kk, vv in fp_hist.items()
                if vv not in [None, -99999, -99999.0]
            }
            for fp_hist in fp_hists
        ]

        # format fp_hists, add alert_mag, alert_ra, alert_dec
        # and computing the FP's mag, magerr, snr, limmag3sig, limmag5sig
        fp_hists = format_fp_hists(alert, fp_hists)

        alert_aux, xmatches, xmatches_ztf, passed_filters = None, None, None, None
        # cross-match with external catalogs if objectId not in collection_alerts_aux:
        if (
            mongo.db[
                collection_alerts_aux
            ].count_documents({"_id": object_id}, limit=1)
            == 0
        ):
            with timer(
                f"Cross-match of {object_id} {candid}", verbose > 1
            ):
                xmatches = alert_filter__xmatch(alert, cross_match_config)

            # Crossmatch new alert with most recent ZTF_alerts and insert
            # with timer(
            #     f"ZTF Cross-match of {object_id} {candid}", verbose > 1
            # ):
            #     xmatches = {
            #         **xmatches,
            #         **alert_filter__xmatch_ztf_alerts(alert),
            #     }

            alert_aux = {
                "_id": object_id,
                "cross_matches": xmatches,
                "prv_candidates": prv_candidates,
                "fp_hists": fp_hists,
            }

            with timer(
                f"Aux ingesting {object_id} {candid}", verbose > 1
            ):
                mongo.insert_one(
                    collection=collection_alerts_aux,
                    document=alert_aux,
                )

        else:
            with timer(
                f"Aux updating of {object_id} {candid}", verbose > 1
            ):
                mongo.db[
                    collection_alerts_aux
                ].update_one(
                    {"_id": object_id},
                    {"$addToSet": {"prv_candidates": {"$each": prv_candidates}}},
                    upsert=True,
                )

                fp_hists = update_fp_hists(alert, fp_hists)

            # Crossmatch exisiting alert with most recent record in ZTF_alerts and update aux
            # with timer(
            #     f"Exists in aux: ZTF Cross-match of {object_id} {candid}",
            #     verbose > 1,
            # ):
            #     xmatches_ztf = alert_filter__xmatch_ztf_alerts(alert)
            #
            # with timer(
            #     f"Aux updating of {object_id} {candid}", verbose > 1
            # ):
            #     mongo.db[
            #         collection_alerts_aux
            #     ].update_one(
            #         {"_id": object_id},
            #         {"$set": {"cross_matches.ZTF_alerts": xmatches_ztf}},
            #         upsert=True,
            #     )

        # if config["misc"]["broker"]:
        #     # execute user-defined alert filters
        #     with timer(
        #         f"Filtering of {object_id} {candid}", verbose > 1
        #     ):
        #         passed_filters = alert_filter__user_defined(
        #             filter_templates, alert
        #         )
        #     if verbose > 1:
        #         log(
        #             f"{object_id} {candid} number of filters passed: {len(passed_filters)}"
        #         )
        #
        #     # post to SkyPortal
        #     alert_sentinel_skyportal(
        #         alert, prv_candidates, passed_filters=passed_filters
        #     )

        # clean up after thyself
        del (
            alert,
            prv_candidates,
            fp_hists,
            xmatches,
            xmatches_ztf,
            alert_aux,
            passed_filters,
            candid,
            object_id,
        )

        return

    file_name, rm_file = argument_list
    try:
        # connect to MongoDB:
        mongo = get_mongo_client()

        # first, we decompress the tar.gz file
        # the result should be a directory with the same name as the file, with the file contents
        # but first check if its not already unpacked:
        dir_name = file_name.replace(".zip", "")

        # the topic should be in the file_name:
        # its either ztf_public or ztf_partnership
        topic = "wtp_all"

        if not os.path.exists(dir_name):
            log(f"Unpacking {file_name}...")
            os.mkdir(dir_name)
            os.system(f"unzip {file_name} -d {dir_name}")
            log(f"Done unpacking {file_name}")

        # grab all the .avro files in the directory:
        avro_files = [str(f) for f in pathlib.Path(dir_name).glob("*.avro")]

        nb_alerts = len(avro_files)
        for i, avro_file in enumerate(avro_files):
            # ingest the avro file:
            with timer(f"Processing alert {i + 1}/{nb_alerts}"):
                try:
                    msg_decoded = decode_message(avro_file)
                    for record in msg_decoded:
                        if (
                            retry(mongo.db[collection_alerts].count_documents)(
                                {"candid": record["candid"]}, limit=1
                            )
                            == 0
                        ):
                            process_alert(
                                record,
                                topic=topic,
                                cross_match_config=cross_match_config,
                            )

                        # clean up after thyself
                        del msg_decoded
                except Exception as e:
                    log(f"Failed to process alert {avro_file}: {str(e)}")
                    continue

            if rm_file:
                os.remove(avro_file)

    except Exception as e:
        traceback.print_exc()
        log(e)
        return

    try:
        if rm_file:
            os.remove(file_name)
            # also remove the directory with the contents
            os.system(f"rm -rf {dir_name}")
    finally:
        pass


def run(
    path: str,
    mindate: str = None,
    maxdate: str = None,
    num_proc: int = multiprocessing.cpu_count(),
    rm: bool = False,
):
    """Preprocess and Ingest ZTF alerts into Kowalski's aux table

    :param path: local path to matchfiles
    :param mindate: min date to process
    :param maxdate: max date to process
    :param num_proc: number of processes to use
    :param rm: remove files after processing
    :return:
    """

    # make sure the path is an absolute path
    path = os.path.abspath(path)

    files = [str(f) for f in pathlib.Path(path).glob("*.zip")]

    # sort the files by date, if provided
    if mindate is not None:
        mindate = datetime.datetime.strptime(mindate, "%Y%m%d")
        files = [
            f
            for f in files
            if mindate
            <= datetime.datetime.strptime(
                pathlib.Path(f).name.split("_")[2].split(".")[0], "%Y%m%d"
            )
        ]
    if maxdate is not None:
        maxdate = datetime.datetime.strptime(maxdate, "%Y%m%d")
        files = [
            f
            for f in files
            if datetime.datetime.strptime(
                pathlib.Path(f).name.split("_")[2].split(".")[0], "%Y%m%d"
            )
            <= maxdate
        ]

    input_list = [(f, rm) for f in files]

    with multiprocessing.Pool(processes=num_proc) as pool:
        for _ in tqdm(pool.imap(process_file, input_list), total=len(files)):
            pass


if __name__ == "__main__":
    fire.Fire(run)
