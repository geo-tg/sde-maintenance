import arcpy
import os
import json
import datetime 


def buildCxn(cfg):

    # Set database connection
    built = 0
    db_cxn_vars = [cfg[x] for x in ["rdbms", "instance", "auth", "un", "pw", "db_name"]]
    if cfg["sde_cxn"]:
        # if cxn string provided, use that
        sde = cfg["sde_cxn"]
    elif all(db_cxn_vars):
        # if no cxn string and all required cxn params
        # provied, build new cxn string
        rdbms, instance, auth, un, pw, db_name = db_cxn_vars
        version = cfg["version"]
        try:
            arcpy.CreateDatabaseConnection_management(sde_cxn_fldr,
                                                f"{un}@{db_name}", 
                                                rdbms, 
                                                instance, 
                                                auth, 
                                                un, 
                                                pw, 
                                                database=db_name,
                                                version=version)
            sde = os.path.join(sde_cxn_fldr, f"{un}@{db_name}.sde")
            built = 1
        except Exception as e:
            print(e)
    else:
        # cannot connect to database
        print("Missing required information to connect to database. \n \
            Confirm the env and config files contain either a connection \n \
            string or information to build a new connection file.")

    return(sde, built)


def reconcileVersions(sde):

    # Set the workspace environment
    arcpy.env.workspace = sde

    # Use a list comprehension to get a list of version names where the owner
    # is the current user and make sure sde.default is not selected.
    # removed ver.isOwner == True, TODO: determine if this works for Solano
    verList = [ver.name for ver in arcpy.da.ListVersions() if 
                ver.name.lower() != 'sde.default'] 

    print('Starting the 1st Reconciliation')

    arcpy.ReconcileVersions_management(sde,
                                    "ALL_VERSIONS",
                                    "SDE.Default",
                                    verList,
                                    "LOCK_ACQUIRED",
                                    "NO_ABORT",
                                    "BY_OBJECT", #TODO: look into for conflicts
                                    "FAVOR_TARGET_VERSION",
                                    "NO_POST",
                                    "KEEP_VERSION",
                                    f"{log_fldr}, RecLog_{timestamp}.txt")
    print('Reconciling Part 1 Complete')

    print('Starting the 2nd Reconciliation with Post')

    arcpy.ReconcileVersions_management(sde,
                                    "ALL_VERSIONS",
                                    "SDE.Default",
                                    verList,
                                    "LOCK_ACQUIRED",
                                    "NO_ABORT",
                                    "BY_OBJECT",
                                    "FAVOR_TARGET_VERSION",
                                    "POST", 
                                    "KEEP_VERSION",
                                    f"{log_fldr}, RecLog_{timestamp}.txt")

    print('2nd Reconciliation of the database has been completed.')
    print('Versions have been posted after reconciliation.')


def compressDB(sde):

    # The database connection file that connects to the enterprise geodatabase to be compressed.
    arcpy.Compress_management(sde) 

    print('Compression complete.')


def rebuildIndex(sde):

    # Set the workspace environment
    arcpy.env.workspace = sde

    # NOTE: Rebuild indexes can accept a Python list of datasets.

    # Get a list of all the datasets the user has access to.
    # First, get all the stand alone tables, feature classes and rasters.
    dataList = arcpy.ListTables() + arcpy.ListFeatureClasses() + arcpy.ListRasters()

    # Next, for feature datasets get all of the datasets and featureclasses
    # from the list and add them to the master list.
    for dataset in arcpy.ListDatasets("", "Feature"):
        arcpy.env.workspace = os.path.join(sde, dataset)
        dataList += arcpy.ListFeatureClasses() + arcpy.ListDatasets()

    # Reset the workspace
    arcpy.env.workspace = sde

    # Get the user name for the workspace
    userName = arcpy.Describe(sde).connectionProperties.user.lower()

    # remove any datasets that are not owned by the connected user.
    userDataList = [ds for ds in dataList if ds.lower().find(f".{userName}.") > -1]

    # Execute rebuild indexes
    # Note: to use the "SYSTEM" option the workspace user must be an administrator.
    arcpy.RebuildIndexes_management(sde, "NO_SYSTEM", userDataList, "ALL")

    print('Rebuild Complete')


def analyzeDatasets(sde):

    # set the workspace environment
    arcpy.env.workspace = sde

    # NOTE: Analyze Datasets can accept a Python list of datasets.

    # Get the user name for the workspace
    userName = arcpy.Describe(sde).connectionProperties.user

    # Get a list of all the datasets the user owns by using a wildcard that 
    # incldues the user name
    # First, get all the stand alone tables, feature classes and rasters.
    dataList = arcpy.ListTables(userName + "*") + \
                arcpy.ListFeatureClasses(userName + "*") + \
                arcpy.ListRasters(userName + "*")

    # Next, for feature datasets get all of the datasets and featureclasses
    # from the list and add them to the master list.
    for dataset in arcpy.ListDatasets(userName + "*", "Feature"):
        arcpy.env.workspace = os.path.join(sde, dataset)
        dataList += arcpy.ListFeatureClasses(userName + "*") + \
                    arcpy.ListDatasets(userName + "*")

    # reset the workspace
    arcpy.env.workspace = sde

    # Execute analyze datasets
    # Note: to use the "SYSTEM" option the workspace user must be an administrator.
    arcpy.AnalyzeDatasets_management(sde, 
                                    "NO_SYSTEM", 
                                    dataList, 
                                    "ANALYZE_BASE",
                                    "ANALYZE_DELTA",
                                    "ANALYZE_ARCHIVE")
    print("Analyze Complete")


def deleteCxn(sde):

    if os.path.exists(sde):
        os.remove(sde)
    

if __name__ == "__main__":

    # timestamp
    timestamp = datetime.datetime.today().strftime("%Y%m%d")

    # set paths
    home_fldr = os.path.dirname(__file__)
    cfg_fldr = os.path.join(home_fldr, "configs")
    env_file = os.path.join(cfg_fldr, "env.json")
    cfg_file = os.path.join(cfg_fldr, "config.json")
    sde_cxn_fldr = os.path.join(home_fldr, "sde_cxn")
    log_fldr = os.path.join(home_fldr, "logs")

    # read configs
    with open(env_file) as f:
        env = (json.load(f))["env"]

    with open(cfg_file) as f:
        cfg = (json.load(f))[env]

    # run funcs
    sde, built = buildCxn(cfg)
    reconcileVersions(sde)
    compressDB(sde)
    rebuildIndex(sde)
    analyzeDatasets(sde)
    if built:
        deleteCxn(sde)

