import arcpy
import os
import os.path
import datetime
import sys

class Logger (object):
    def __init__(self, fileName, printingEnabled=True, startingMessage=None):
        logsFolderPath = r"\\san1\SIR\SystemInformation\System Information Group\GIS\SIRgisAdmin\OffHoursLogs"
        
        now = datetime.datetime.now() # current date and time
        date_time = now.strftime("%B_%d_%Y_TIME-%HH_%MM_%SS")
        logFilePath = os.path.join(logsFolderPath, date_time + "_" + fileName + ".txt" )
        
        self.errorLog = open(logFilePath, "w+")
        self.printingEnabled = printingEnabled
        self.log("Error Log Started at")
        
        if not startingMessage == None:
            self.log(startingMessage)
        
        
    def log(self, comment, header=False, indent=False):
        timestampTxt = datetime.datetime.now().strftime("%B_%d_%Y_TIME-%HH_%MM_%SS")
        
        if indent:
            indentTxt = "      "
        else:
            indentTxt = ""
            
        if header:
            headerText = "***********************************************************************************************************************************************************************************"
        else:
            headerText = ""
        
        statement = "{ident}{timestamp} - {comment} {header}".format(timestamp = timestampTxt, comment = comment, ident=indentTxt, header=headerText)   
        self.errorLog.write(statement + "\n")
        if self.printingEnabled:
            print statement
            
    def close(self):
        self.errorLog.close()
        del self.errorLog


def updateRoadInventory(roadInventoryTableUpdate=False, nhsUpdate=True, aphnUpdate=True, FunctionalClassUpdate=True, specialSystemUpdate=True, routeSignUpdate=True, roadInventoryFCUpdate=True, ARNOLDmatch=True):
    
    #setting up things for afterhours logs
    logger = Logger("RoadInvUpdate")
    logger.log("Road Inventory Updated Started at", header=True)
    
    #parameter paths for execusion of script
    scriptDirectry = os.path.dirname(sys.argv[0])
    outputTableMXDPath = os.path.join(scriptDirectry, "outputTable.mxd")
    outputMXD = arcpy.mapping.MapDocument(outputTableMXDPath)
    outputView = arcpy.mapping.ListTableViews(outputMXD)[0]
    logger.log("output table has been found", indent=True)
    
    #Dates table
    dateTableMXDPath = os.path.join(scriptDirectry, "datesTable.mxd")
    dateMXD = arcpy.mapping.MapDocument(dateTableMXDPath)
    dateTableView = arcpy.mapping.ListTableViews(dateMXD)[0]
    
    def getMergeRoadInventoryDate():
        outputDate = None
        with arcpy.da.SearchCursor(dateTableView, ["dataset", "updateDate"]) as mergeRICur:
            for row in mergeRICur:
                if row[0] == "Road Inventory Merge Table":
                    outputDate = row[1]
        
        return outputDate
                    
    
    def updateDate(datasetName, date=None):
        if date == None:
            date = getMergeRoadInventoryDate()
        
        
        with arcpy.da.UpdateCursor(dateTableView, ["dataset", "updateDate"]) as dateCur:
            for row in dateCur:
                if row[0] == datasetName:
                    row[1] = date
                    dateCur.updateRow(row)
    
    if roadInventoryTableUpdate:
        logger.log("Road Inventory Table Will be updated.", header=True)
        
        #find and clear the merge table
        logger.log("Selecting rows in output table", indent=True)
        arcpy.SelectLayerByAttribute_management(outputView, "NEW_SELECTION")
        logger.log("Deleting rows in output table", indent=True)
        arcpy.DeleteRows_management(outputView)
    
        #find input tables
        logger.log('retreiving table views table view', indent=True)
        inputTablesMXDPath = os.path.join(scriptDirectry, "inputTables.mxd")
        mxd = arcpy.mapping.MapDocument(inputTablesMXDPath)
        viewsList = arcpy.mapping.ListTableViews(mxd)
        gisTableView = viewsList[0]
        legacyTableView = viewsList[1]
        
        #off system append
        logger.log('Selecting off-system Road Inventory Table.', indent=True)
        arcpy.SelectLayerByAttribute_management(gisTableView, "NEW_SELECTION")
        logger.log('Appending GIS Data')
        arcpy.Append_management([gisTableView ], outputView, 'NO_TEST')
        logger.log('clearing off-system selection', indent=True)
        arcpy.SelectLayerByAttribute_management(gisTableView, "CLEAR_SELECTION")
        del gisTableView
        
        #on system append
        logger.log('selecting on system table', indent=True)
        arcpy.SelectLayerByAttribute_management(legacyTableView, "NEW_SELECTION")
        logger.log('appending on system table', indent=True)
        arcpy.Append_management([legacyTableView], outputView, 'NO_TEST')
        logger.log('Clearing on system table', indent=True)
        arcpy.SelectLayerByAttribute_management(legacyTableView, "CLEAR_SELECTION")
        del legacyTableView
        
        #update the update date table
        updateDate("Road Inventory Merge Table", datetime.datetime.now().strftime("%m/%d/%Y"))
        updateDate("Road Inventory Merge Table", datetime.datetime.now().strftime("%m/%d/%Y"))
        
        
    #ARNOLD feature class
    arnoldLayerFilePath = os.path.join(scriptDirectry, "ARNOLD.lyr")
    commonEventProperties = "AH_RoadID LINE AH_BLM AH_ELM"
        
    if nhsUpdate or aphnUpdate or FunctionalClassUpdate or specialSystemUpdate or routeSignUpdate or roadInventoryFCUpdate or ARNOLDmatch:
        
        logger.log('a dissolve update of some kind has been selected. starting to make local copy of road inventory database to improve processing speed.')
        arcpy.SelectLayerByAttribute_management(outputView, "NEW_SELECTION")
        arcpy.TableToTable_conversion(outputView, r"in_memory", "RoadInv")
        arcpy.MakeTableView_management(r"in_memory\RoadInv", "RoadInvTableView")
        outputView = "RoadInvTableView"
        #arcpy.AddIndex_management(r"in_memory\RoadInv", "ID", "roadInvID")#, "NON_UNIQUE", "ASCENDING")
        
        
        #defining function to perform dissolves on multiple layers
        def createDissolve(outputLayerFileList, constrainSQL, dissolveField):
            tempPath = r"in_memory\Dissolve"
            tempPath2 = r"in_memory\Dissolve2"
            #tempPath = r"C:\scratch\ADT\turn.gdb\Dissolve"
            #tempPath2 = r"C:\scratch\ADT\turn.gdb\Dissolve2"
            
            if arcpy.Exists(tempPath):
                arcpy.Delete_management(tempPath)
            if arcpy.Exists(tempPath2):
                arcpy.Delete_management(tempPath2)
                
                
            logger.log("selecting table features", indent=True)
            arcpy.SelectLayerByAttribute_management(outputView, "NEW_SELECTION", where_clause=constrainSQL)
            
            #dissolve accordingly
            logger.log("dissolving routes", indent=True)
            arcpy.DissolveRouteEvents_lr(in_events = outputView, in_event_properties = commonEventProperties ,dissolve_field = ["AH_District", "AH_County", "AH_Route", "AH_Section", "LOG_DIRECT", dissolveField] , out_table = tempPath , out_event_properties = commonEventProperties, dissolve_type= "DISSOLVE")
            
            #round out segments with more than 3 decimal places, remove all zero length segments
            logger.log("rounding dissolved routes", indent=True)
            with arcpy.da.UpdateCursor(tempPath, ["AH_BLM" , "AH_ELM"]) as upCur:
                for row in upCur:
                    row[0] == round(row[0], 3)
                    row[1] == round(row[1], 3)
                    
                    if round(row[0], 3) == round(row[1], 3):
                        upCur.deleteRow()
                    else:
                        upCur.updateRow(row)
                        
            #turn into route event
            logger.log("making route event", indent=True)
            arcpy.MakeRouteEventLayer_lr(in_routes=arcpy.mapping.Layer(arnoldLayerFilePath), route_id_field="AH_RoadID", in_table=tempPath, in_event_properties = commonEventProperties, out_layer=dissolveField + "_event", add_error_field="NO_ERROR_FIELD", add_angle_field="NO_ANGLE_FIELD")
            
            #export route event to Feature class
            logger.log( "exporting route event" , indent=True)
            arcpy.FeatureClassToFeatureClass_conversion(dissolveField + "_event", os.path.split(tempPath2)[0], os.path.split(tempPath2)[1])
            
            
            logger.log( "adding and populating length field" , indent=True)
            arcpy.AddField_management(tempPath2, "AH_Length", "DOUBLE", 38, 8)
            
            with arcpy.da.UpdateCursor(tempPath2, ["AH_Length", "AH_BLM", "AH_ELM"]) as lengthCur:
                for row in lengthCur:
                    if not row[1] == None and not row[2] == None and row[1] >= 0 and row[2] >= 0:
                        row[0] = round(row[2] - row[1], 3)
                        lengthCur.updateRow(row)
                        
            #append to data source
            logger.log( "appending into final source" , indent=True)
            for outputLayerFile in outputLayerFileList:
                finalLayer = arcpy.mapping.Layer(outputLayerFile)
                arcpy.DeleteFeatures_management(finalLayer)
                arcpy.Append_management(tempPath2, finalLayer, "NO_TEST")
                
            logger.log("Deleting temp layers related to dissolve", indent=True)
            arcpy.Delete_management(tempPath)
            arcpy.Delete_management(tempPath2)
            
            
            
        if nhsUpdate:
            #NHS
            logger.log("doing NHS", header=True)
            logger.log("starting NHS dissolve.", indent=True)
            NHSLayerFilePath = os.path.join(scriptDirectry, "NHS_SQLGIS.lyr")
            dissolveFieldNHS = "NHS"
            sqlConstraintNHS = "SystemStatus = '1' AND NHS IN ('1', '10', '2', '3' , '4', '5', '6', '7', '8', '9') AND TypeRoad IN ('1', '2')"
            createDissolve([NHSLayerFilePath], sqlConstraintNHS, dissolveFieldNHS)
            
            #populate intermodal field
            logger.log("Populating intermodal Field", indent=True)
            with arcpy.da.UpdateCursor(arcpy.mapping.Layer(NHSLayerFilePath), [ "NHS" , "Intermodal"]) as nhsCur:
                for row in nhsCur:
                    if row[0] == "1" or row[0] == "10":
                        row[1] = "0"
                    else:
                        row[1] == '1'
                        
                    nhsCur.updateRow(row)
                    
            updateDate("NHS Dissolve")
                    
            
        if aphnUpdate:
            #aphn feature class
            logger.log("doing APHN", header=True)
            logger.log("starting APHN dissolve.", indent=True)
            aphnLayerFilePath = os.path.join(scriptDirectry, "APHN_SQLGIS.lyr")
            dissolveFieldAPHN = "APHN"
            sqlConstraint = "SystemStatus = '1' AND APHN IN ('1', '2', '3' , '4') AND TypeRoad IN ('1', '2')"
            createDissolve([aphnLayerFilePath], sqlConstraint, dissolveFieldAPHN)
            
            updateDate("APHN Dissolve")
            
        if specialSystemUpdate:
            #special System
            logger.log("doing special systems", header=True)
            logger.log("starting Special System dissolve.", indent=True)
            specialSystemFilePath = os.path.join(scriptDirectry, "SpecialSystem_SQLGIS.lyr")
            dissolveFieldSpecialSystem = "SpecialSystem"
            sqlConstraintSpecialSystem = "SystemStatus = '1' AND SpecialSystem IN ('3', '4', '5', '6', '7', '9') AND TypeRoad IN ('1', '2')"
            createDissolve([specialSystemFilePath], sqlConstraintSpecialSystem, dissolveFieldSpecialSystem)
            
            updateDate("Special System Dissolve")
            
        if FunctionalClassUpdate:
            #Functional Class
            logger.log("doing functional class", header=True)
            logger.log("starting functional class dissolve.", indent=True)
            functionalClassLayerFilePath = os.path.join(scriptDirectry, "FunctionalClass_SQLGIS.lyr")
            dissolveFieldFunctionalClass = "FuncClass"
            sqlConstraintFunct = "SystemStatus = '1' AND FuncClass IN ('1', '2' , '3', '4', '5' , '6') AND TypeRoad IN ('1', '2')"
            createDissolve([functionalClassLayerFilePath], sqlConstraintFunct, dissolveFieldFunctionalClass)
            
            updateDate("Functional Class Dissolve")
            
        if routeSignUpdate:
            #RouteSign
            logger.log("doing route sign", header=True)
            logger.log("starting Route Sign dissolve.", indent=True)
            routeSignLayerFilePath = os.path.join(scriptDirectry, "RouteSign_SQLGIS.lyr")
            dissolveFieldRouteSign = "RouteSign"
            sqlConstraintSpecialSystem = "SystemStatus = '1' AND RouteSign IN ('1', '2', '3', '4', '5') AND TypeRoad IN ('1', '2')"
            createDissolve([routeSignLayerFilePath], sqlConstraintSpecialSystem, dissolveFieldRouteSign)
            
            updateDate("Route Sign Dissolve")
            
        if roadInventoryFCUpdate:
            #deal with road inventory_FC
            logger.log("Road Inventory Feature Class Export", header=True)
            RoadInv_FC_LayerObject = arcpy.mapping.Layer(os.path.join(scriptDirectry, "RoadInventory_FC2_SQLGIS.lyr"))
            roadInvMappedRouteEvent = "RoadInv_FC"
            logger.log("Making route event", indent=True)
            arcpy.MakeRouteEventLayer_lr(in_routes=arcpy.mapping.Layer(arnoldLayerFilePath), route_id_field="AH_RoadID", in_table=outputView, in_event_properties = commonEventProperties, out_layer=roadInvMappedRouteEvent, add_error_field="ERROR_FIELD", add_angle_field="NO_ANGLE_FIELD")
            logger.log("Exporting route feature class", indent=True)
            arcpy.FeatureClassToFeatureClass_conversion(roadInvMappedRouteEvent, "in_memory", "RoadInv_FC")
            logger.log("Adding Length Field", indent=True)
            arcpy.AddField_management(r"in_memory\RoadInv_FC", "AH_Length", "DOUBLE", 8)
            
            #update AH_Length Field
            logger.log("Populating Length Field", indent=True)
            with arcpy.da.UpdateCursor(r"in_memory\RoadInv_FC", ["AH_Length", "AH_BLM", "AH_ELM"]) as lengthCur:
                for row in lengthCur:
                    if not row[1] == None and not row[2] == None:
                        row[0] = row[2] - row[1]
                        lengthCur.updateRow(row)
            
            logger.log("claring target datasource", indent=True)
            arcpy.DeleteFeatures_management(RoadInv_FC_LayerObject)
            
            logger.log("appending data", indent=True)
            arcpy.Append_management("in_memory\RoadInv_FC", RoadInv_FC_LayerObject, "NO_TEST")
            
            logger.log("Deleting in memory data product", indent=True)
            arcpy.Delete_management("in_memory\RoadInv_FC")
            print datetime.datetime.now()
            
            updateDate("Road Inventory Feature Class")
            
        if ARNOLDmatch:
            logger.log("starting arnold match search", header=True)
            
            #output union table location
            arnoldTablePath = r"C:\scratch\ADT\IRtest.gdb\ARNOLD_Export"
            ARNOLDdissolvePath = r"C:\scratch\ADT\IRtest.gdb\ARNOLD_Dissolve"
            matchErrorTable = r"C:\scratch\ADT\IRtest.gdb\matchErrorsTable"
            matchErrorEvent = "errors event"
            matchErrorFeatureClassPath = r"C:\scratch\ADT\IRtest.gdb\matchErrors_FC"
            intersectPath = r"C:\scratch\ADT\IRtest.gdb\IntersectTable"
            intersectView = "IntersectTable"
            
            arnoldSplitsRoadInv = r"C:\scratch\ADT\IRtest.gdb\arnoldSplitsRoadInv"
            roadInvIdsStat = r"C:\scratch\ADT\IRtest.gdb\roadInvIds"
            
            for item in [arnoldTablePath, ARNOLDdissolvePath, matchErrorTable, matchErrorFeatureClassPath, intersectPath, arnoldSplitsRoadInv, roadInvIdsStat]:
                if arcpy.Exists(item):
                    logger.log("deleting: {}".format(item), indent=True)
                    arcpy.Delete_management(item)
                    
            
            #build the output table
            logger.log("Creating ARNOLD match table", indent=True)
            arcpy.CreateTable_management(os.path.split(matchErrorTable)[0], os.path.split(matchErrorTable)[1])
            arcpy.AddField_management(matchErrorTable, "AH_RoadID", "TEXT", field_length=150)
            arcpy.AddField_management(matchErrorTable, "AH_BLM", "DOUBLE")
            arcpy.AddField_management(matchErrorTable, "AH_ELM", "DOUBLE")
            arcpy.AddField_management(matchErrorTable, "AH_BLM_Suggest", "DOUBLE")
            arcpy.AddField_management(matchErrorTable, "AH_ELM_Suggest", "DOUBLE")
            arcpy.AddField_management(matchErrorTable, "BLM_DIFF", "DOUBLE")
            arcpy.AddField_management(matchErrorTable, "ELM_DIFF", "DOUBLE")
            arcpy.AddField_management(matchErrorTable, "RD_DESIGN", "TEXT", field_length=25)
            arcpy.AddField_management(matchErrorTable, "AH_Signed", "TEXT", field_length=10)
            arcpy.AddField_management(matchErrorTable, "RouteSign", "TEXT", field_length=2)
            arcpy.AddField_management(matchErrorTable, "ERROR", "TEXT", field_length=150)

            
            #export ARNOLD to an in memory non-spatial table
            logger.log("Creating ARNOLD Layer Object", indent=True)
            arnoldFeatureClassLayer = arcpy.mapping.Layer(arnoldLayerFilePath)
            logger.log("Selecting all ARNOLD features", indent=True)
            arcpy.SelectLayerByAttribute_management(arnoldFeatureClassLayer, "NEW_SELECTION")
            arcpy.TableToTable_conversion(arnoldFeatureClassLayer, os.path.split(arnoldTablePath)[0], os.path.split(arnoldTablePath)[1])
            
            #dissolve ARNOLD so it can become a constraining element
            logger.log("Dissolving ARNOLD tabularly", indent=True)
            arcpy.DissolveRouteEvents_lr(arnoldTablePath, commonEventProperties, ["AH_County", "AH_Route", "AH_Section"], ARNOLDdissolvePath, commonEventProperties)
            arcpy.AddField_management(ARNOLDdissolvePath, "ARNOLD_dissolveID", "TEXT")
            arcpy.CalculateField_management(ARNOLDdissolvePath, "ARNOLD_dissolveID", "!OBJECTID!", "PYTHON")
            
            
            #run overlay between ARNOLD and merged road inventory
            logger.log("Selecting Road Inventory Table", indent=True)
            arcpy.SelectLayerByAttribute_management(outputView, "NEW_SELECTION")
            logger.log("Performing route overlay", indent=True)
            arcpy.OverlayRouteEvents_lr(outputView, commonEventProperties, ARNOLDdissolvePath, commonEventProperties, "UNION", intersectPath, commonEventProperties, "NO_ZERO", "FIELDS", "INDEX")
            arcpy.AddField_management(intersectPath, "roadIDinRoadInv", 'TEXT' , field_length=2)
            
            #get records that are split by ARNOLD
            splitByARNOLDList = []
            arcpy.Statistics_analysis(intersectPath, arnoldSplitsRoadInv, [[ "ID" , "COUNT"]] , ["ID"])
            with arcpy.da.SearchCursor(arnoldSplitsRoadInv , ["ID"], where_clause="FREQUENCY > 1" ) as statCur:
                for row in statCur:
                    splitByARNOLDList.append(row[0])
                    

            
            
            logger.log("Turning route overlay into table view", indent=True)
            print "intersect path"
            print intersectPath
            print "intersect view"
            print intersectView
            arcpy.MakeTableView_management(intersectPath, intersectView)
            
            logger.log("Adding ID indexies before join is applied", indent=True)   
            arcpy.AddIndex_management(intersectView, "ID", "roadInvID", "NON_UNIQUE", "ASCENDING")
            arcpy.AddIndex_management(intersectView, "AH_RoadID", "roadID", "NON_UNIQUE", "ASCENDING")
            
            logger.log("Getting roadIDs list", indent=True)
            arcpy.Statistics_analysis(outputView, roadInvIdsStat, [["AH_RoadID" , "Count"]] , ["AH_RoadID"])
            arcpy.MakeTableView_management(roadInvIdsStat , "roadInvIdStat")
            
            
            logger.log("populating in road inventory field", indent=True)

            arcpy.AddJoin_management(intersectView, "AH_RoadID", "roadInvIdStat" , "AH_RoadID" , "KEEP_COMMON")
            arcpy.CalculateField_management(intersectView, "{}.roadIDinRoadInv".format(intersectView) , " 'y' ")
            arcpy.RemoveJoin_management(intersectView)
            
            
            
            roadIDlist = []#consider replaceing with summary statistics tool to improve overall speed
            with arcpy.da.SearchCursor(outputView, ["AH_RoadID"]) as roadIDcur:
                for row in roadIDcur:
                    if row[0] != None and row[0] != "":
                        roadIDlist.append(str(row[0]))
                        roadIDlist = list(set(roadIDlist))
            roadInvRoadIDsSQL = " {}.AH_RoadID IN {}".format(intersectView, str(tuple(roadIDlist)))
                    
            
            logger.log("Selecting join table", indent=True)
            arcpy.AddJoin_management(outputView, "ID", intersectView, "ID" , "KEEP_COMMON")
            #arcpy.SelectLayerByAttribute_management(outputView, "NEW_SELECTION", '"SIR.DBO.RoadInventoryTableMerge.AH_BLM" <> "{viewname}.AH_BLM" OR "SIR.DBO.RoadInventoryTableMerge.AH_ELM" <> "{viewname}.AH_ELM"  '.format(viewname=intersectView))
            
            insertCur = arcpy.da.InsertCursor(matchErrorTable, ["AH_RoadID", "AH_BLM", "AH_ELM" , "AH_BLM_Suggest", "AH_ELM_Suggest", "BLM_DIFF", "ELM_DIFF", "RouteSign", "ERROR"])
            
            for field in arcpy.ListFields(outputView):
                print "{} | {} | {}".format(field.name, field.type, field.length)
            
            logger.log("Adding features to match table", indent=True)
            with arcpy.da.SearchCursor(outputView, ["RoadInv.AH_RoadID", "RoadInv.AH_BLM" , "RoadInv.AH_ELM", "{}.AH_BLM".format(intersectView), "{}.AH_ELM".format(intersectView), "RoadInv.RouteSign", "RoadInv.ID"], where_clause=" RoadInv.ID <> '' AND {}.ARNOLD_dissolveID <> '' ".format(intersectView)) as overshootCur:
                for row in overshootCur:
                    AH_RoadID = row[0]
                    AH_BLM = row[1]
                    AH_ELM = row[2]
                    suggestBLM = row[3]
                    suggestELM = row[4]
                    routeSign = row[5]
                    ID = row[6]
                    
                    if ID in splitByARNOLDList:
                        insertCur.insertRow([AH_RoadID, AH_BLM, AH_ELM, suggestBLM, suggestELM,  suggestBLM - AH_BLM,  AH_ELM - suggestELM  , routeSign, "Road Inv Record spans ARNOLD gap" ])
                    elif AH_BLM != suggestBLM and AH_ELM != suggestELM:
                        #["AH_RoadID", "AH_BLM", "AH_ELM" , "AH_BLM_Suggest", "AH_ELM_Suggest", "BLM_DIFF", "ELM_DIFF", "RD_DESIGN", "routeSign", "ERROR"]
                        insertCur.insertRow([AH_RoadID, AH_BLM, AH_ELM, suggestBLM, suggestELM,  suggestBLM - AH_BLM,  AH_ELM - suggestELM  , routeSign, "BLM and ELM overshoots ARNOLD" ])
                    elif AH_BLM != suggestBLM:
                        insertCur.insertRow([AH_RoadID, AH_BLM, AH_ELM, suggestBLM, suggestELM,  suggestBLM - AH_BLM,  None  , routeSign, "BLM overshoots ARNOLD" ])
                        
                    elif AH_ELM != suggestELM:
                        insertCur.insertRow([AH_RoadID, AH_BLM, AH_ELM, suggestBLM, suggestELM,  None,  AH_ELM - suggestELM  ,  routeSign, "ELM overshoots ARNOLD" ])
                        
            del insertCur
            
            gapInsertCur = arcpy.da.InsertCursor(matchErrorTable, ["AH_RoadID", "AH_BLM", "AH_ELM", "ERROR" ])
            
            #dissolve road inventory
            #intersect road inventory with
            sqlForGapConstraint = " RoadInv.ID = '' AND {}.ARNOLD_dissolveID <> '' AND roadIDinRoadInv = 'y' ".format(intersectView)
            with arcpy.da.SearchCursor(outputView, [ "{}.AH_RoadID".format(intersectView) , "{}.AH_BLM".format(intersectView), "{}.AH_ELM".format(intersectView)], where_clause=sqlForGapConstraint ) as gapCur:
                for row in gapCur:
                    AH_RoadID = row[0]
                    AH_BLM = row[1]
                    AH_ELM = row[2]
                    #["AH_RoadID", "AH_BLM", "AH_ELM" , "AH_BLM_Suggest", "AH_ELM_Suggest", "BLM_DIFF", "ELM_DIFF", "RD_DESIGN", "routeSign", "ERROR"]
                    #more code needed here to intert gaps in coverage
                    gapInsertCur.insertRow([AH_RoadID, AH_BLM, AH_ELM, 'Possible Gap on existing Road Inventory covered Road'])
                    
            del gapInsertCur
            
            
            arcpy.RemoveJoin_management(outputView)
            arcpy.MakeRouteEventLayer_lr(arnoldFeatureClassLayer, "AH_RoadID", matchErrorTable, commonEventProperties, matchErrorEvent)
            arcpy.FeatureClassToFeatureClass_conversion(matchErrorEvent, os.path.split(matchErrorFeatureClassPath)[0] , os.path.split(matchErrorFeatureClassPath)[1])
            
            
            
            #query the resulting table for two times, once for records that are not in road inventory, then for road inventory that is not in ARNOLD
            #or
            #create table of routes in road inventory ((NOT ID = '') AND (NOT AH_ID = '')) and table of ARNOLD records outside of it ( NOT ID = '') AND (AH_ID = '').
            #Then join the two table, the few joined records to account for areas where road inventory overruns ARNOLD.
            #or
            #dissolve ARNOLD into a single table
            #then route overlay (intersect) with road inventory
            #then proceed to join the original Road inventory record with the intersect by ID
            #query for records where AH_BLM does not match do find BLM overshooting records
            #query for records where AH_ELM does not match ELM to find ELM overshooting records
            
            #to find undershooting records
            #dissolve road inventory by route ID
            #the intersect with ARNOLD segments (undissolved)
            #join result to to ARNOLD by AH_ID
            #query for records where the BLM does not match between the two tables, these should be your undershot BLM records
            
            
            
    logger.log("Script Finished!!!")
    logger.close()

updateRoadInventory(roadInventoryTableUpdate=True, 
    nhsUpdate=True, 
    aphnUpdate=True, 
    FunctionalClassUpdate=False, 
    specialSystemUpdate=False, 
    routeSignUpdate=False, 
    roadInventoryFCUpdate=False,
    ARNOLDmatch=False)

#update the road inventory or dissolves
# updateRoadInventory(roadInventoryTableUpdate=False, 
    # nhsUpdate=False, 
    # aphnUpdate=False, 
    # FunctionalClassUpdate=False, 
    # specialSystemUpdate=False, 
    # routeSignUpdate=False, 
    # roadInventoryFCUpdate=False,
    # ARNOLDmatch=True)
    
    
