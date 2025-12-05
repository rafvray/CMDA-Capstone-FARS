-- =====================================================
-- Schema Relationships (Foreign Key Overview)
-- =====================================================
-- accident_master
--   • Primary Key: ST_CASE
--   • Parent table for vehicle_master and person_master
--
-- vehicle_master
--   • Primary Key: (ST_CASE, VEH_NO)
--   • Foreign Key: ST_CASE → accident_master(ST_CASE)
--   • Parent table for person_master
--
-- person_master
--   • Primary Key: (ST_CASE, VEH_NO, PER_NO)
--   • Foreign Keys:
--       ST_CASE → accident_master(ST_CASE)
--       (ST_CASE, VEH_NO) → vehicle_master(ST_CASE, VEH_NO)
--
-- Relationship Summary:
--   One crash (accident_master) → many vehicles (vehicle_master)
--   One vehicle (vehicle_master) → many persons (person_master)
--   One crash (accident_master) → many persons (person_master)
-- =====================================================


-- =====================================================
-- Table: accident_master
-- Description: Contains information about crashes.
-- Primary Key: ST_CASE (Crash Case Number)
-- Source: Fatality Analysis Reporting System (FARS)
-- =====================================================
CREATE TABLE `accident_master` (
  `ST_CASE` bigint NOT NULL, -- Unique case number assigned to each crash record (Primary Key)
  
  `PERNOTMVIT` double DEFAULT NULL, -- C3A: Number of persons not in motor vehicles in transport (e.g., pedestrians, cyclists)
  `VE_TOTAL` double DEFAULT NULL, -- C4: Total number of vehicle forms submitted (all vehicles involved)
  `VE_FORMS` bigint DEFAULT NULL, -- C4A: Number of motor vehicles in transport (MVIT)
  `PVH_INVL` double DEFAULT NULL, -- C4B: Number of parked or working vehicles involved in the crash
  `PERSONS` bigint DEFAULT NULL, -- C5: Total number of person forms submitted (all persons involved)
  `PERMVIT` double DEFAULT NULL, -- C5A: Number of persons in motor vehicles in transport (MVIT)
  
  `COUNTY` bigint DEFAULT NULL, -- C6: County where the crash occurred
  `CITY` bigint DEFAULT NULL, -- C7: City where the crash occurred
  `MONTH` bigint DEFAULT NULL, -- C8A: Month of crash (1 = January, 2 = February, 3 = March, 4 = April, 5 = May, 6 = June, 7 = July, 8 = August, 9 = September, 10 = October, 11 = November, 12 = December)
  `DAY` bigint DEFAULT NULL, -- C8B: Day of crash (1–31 = Day of the month)
  `DAY_WEEK` bigint DEFAULT NULL, -- C8C: Day of the week (1 = Sunday, 2 = Monday, 3 = Tuesday, 4 = Wednesday, 5 = Thursday, 6 = Friday, 7 = Saturday)
  `YEAR` bigint DEFAULT NULL, -- C8D: Year of crash
  `HOUR` bigint DEFAULT NULL, -- C9A: Hour of crash (0–23 = Hour of crash, 99 = Unknown)
  `MINUTE` bigint DEFAULT NULL, -- C9B: Minute of crash (0–59 = Minute, 99 = Unknown)
  
  `TWAY_ID` text, -- C10: Primary trafficway identifier (e.g., street or highway name)
  `TWAY_ID2` text, -- C10: Secondary trafficway identifier (e.g., intersecting street)
  `CL_TWAY` double DEFAULT NULL, -- Not in FARS manual: likely classification or type of trafficway
  
  `ROUTE` double DEFAULT NULL, -- C11: Route signing (e.g., Interstate, U.S., State, County)
  `RUR_URB` double DEFAULT NULL, -- C12A: Rural/Urban classification of the crash location (1 = Rural, 2 = Urban, 6 = Trafficway Not in State Inventory, 8 = Not Reported, 9 = Unknown)
  `FUNC_SYS` double DEFAULT NULL, -- C12B: Functional system (e.g., Interstate, Principal Arterial, Local Road)
  `RD_OWNER` double DEFAULT NULL, -- C13: Road ownership (e.g., State, County, City)
  `NHS` double DEFAULT NULL, -- C14: National Highway System indicator (1 = On NHS, 0 = Not on NHS, 9 = Unknown)
  `SP_JUR` bigint DEFAULT NULL, -- C15: Special jurisdiction (0 = No Special Jurisdication, 1 = National Park Service, 2 = Military, 3 = Indian Reservation, 4 = College/University Campus, 5 = Other Federal Properties, 6 = Other, 9 = Unknown)
  `MILEPT` double DEFAULT NULL, -- C16: Milepoint of crash location
  
  `LATITUDE` double DEFAULT NULL, -- C17A: Latitude of crash location
  `LONGITUD` double DEFAULT NULL, -- C17B: Longitude of crash location
  
  `HARM_EV` bigint DEFAULT NULL, -- C19: First harmful event (the event that caused the first injury or damage)
  `MAN_COLL` bigint DEFAULT NULL, -- C20: Manner of collision for the first harmful event (e.g., front-to-front, angle, sideswipe)
  
  `RELJCT1` double DEFAULT NULL, -- C21A: Relation to junction—within interchange area (yes/no/unknown)
  `REL_JUNC` double DEFAULT NULL, -- Possibly alternate field name for RELJCT1 (relation to junction)
  `RELJCT2` double DEFAULT NULL, -- C21B: Relation to junction—specific location (e.g., intersection, ramp, service road)
  `TYP_INT` double DEFAULT NULL, -- C22: Type of intersection (1 = Not an Intersection, 2 = Four-Way Intersection, 3 = T-Intersection, 4 = Y-Intersection, 5 = Traffic Circle, 6 = Roudabout, 7 = Five-Point, 10 = L-Intersection, 98 = Not Reported, 99 = Unknown)
  `REL_ROAD` bigint DEFAULT NULL, -- C23: Relation to trafficway (e.g., on roadway, shoulder, median, roadside)
  `C_M_ZONE` double DEFAULT NULL, -- Not in FARS manual: possibly construction/maintenance zone indicator
  `WRK_ZONE` double DEFAULT NULL, -- C24: Work zone indicator (0 = None, 1 = Construction, 2 = Maintenance, 3 = Utility, 4 = Work Zone)
  
  `LGT_COND` bigint DEFAULT NULL, -- C25: Light condition (e.g., daylight, dark but lighted, dark unlighted)
  `WEATHER` bigint DEFAULT NULL, -- C26: Atmospheric conditions (1 = Clear, 2 = Rain, 3 = Sleet/Hail, 4 = Snow, 5 = Fog/Smog/Smoke, 6 = Severe Crosswinds, 7 = Blowing Sand/Soil/Dirt, 8 = Other, 10 = Cloudy, 11 = Blowing Snow, 12 = Freezing Rain/Drizzle, 98 = Not reported, 99 = Unknown)
  `SCH_BUS` double DEFAULT NULL, -- C27: School bus related (1 = Yes, 0 = No)
  
  `RAIL` text, -- C28: Rail grade crossing identifier (unique code for railroad crossing if applicable)
  
  `NOT_HOUR` bigint DEFAULT NULL, -- C29A: Hour of notification (when authorities were notified) (0-23 = Hour)
  `NOT_MIN` bigint DEFAULT NULL, -- C29B: Minute of notification (0-59 = Minute)
  `ARR_HOUR` bigint DEFAULT NULL, -- C30A: Hour of arrival at scene (first responder) (0-23 = Hour)
  `ARR_MIN` bigint DEFAULT NULL, -- C30B: Minute of arrival at scene (0-59 = Minute)
  `HOSP_HR` double DEFAULT NULL, -- C31A: Hour of EMS arrival at hospital (0-23 = Hour)
  `HOSP_MN` double DEFAULT NULL, -- C31B: Minute of EMS arrival at hospital (0-59 = Minute)
  
  `FATALS` bigint DEFAULT NULL, -- C101: Number of fatalities in the crash
  
  PRIMARY KEY (`ST_CASE`)
);

-- =====================================================
-- Table: vehicle_master
-- Description: Contains information about vehicles involved in crashes.
-- Primary Key: ST_CASE (Crash Case Number)
-- Source: Fatality Analysis Reporting System (FARS)
-- =====================================================
CREATE TABLE `vehicle_master` (
  `ST_CASE` bigint NOT NULL, -- Unique case number (links to accident_master; primary key)
  
  `OCUPANTS` double DEFAULT NULL, -- V4: Number of occupants (includes driver + passengers)
  `NUMOCCS` double DEFAULT NULL, -- V4: Duplicate count of occupants (99 = Unknown)
  `UNITTYPE` double DEFAULT NULL, -- V5: Unit type (1 = Motor Vehicle In-Transport)
  `HIT_RUN` bigint DEFAULT NULL, -- V6: Hit-and-run indicator (0 = No, 1 = Yes)
  `REG_STAT` double DEFAULT NULL, -- V7: Vehicle registration state
  `OWNER` double DEFAULT NULL, -- V8: Registered vehicle owner type (0 = Not Applicable/Vehicle Not Registed, 1 = Driver was Registered Owner, 2 = Driver Not Registered Owner, 3 = Vehicle Registered as Commercial/Business/Company/Government Vehicle, 4 = Vehicle Registered as Rental Vehicle, 5 = Vehicle was Stolen, 6 = Driverless, 9 = Unknown)
  `VIN` text, -- V9: Vehicle Identification Number (VIN)
  `MOD_YEAR` bigint DEFAULT NULL, -- V10: Vehicle model year
  `VPICMAKE` double DEFAULT NULL, -- V11: vPIC make (from NHTSA vPIC database)
  `VPICMODEL` double DEFAULT NULL, -- V12: vPIC model
  `VPICBODYCLASS` double DEFAULT NULL, -- V13: vPIC body class
  `MAKE` bigint DEFAULT NULL, -- V14: NCSA make code
  `MODEL` bigint DEFAULT NULL, -- V15: NCSA model code
  `BODY_TYP` bigint DEFAULT NULL, -- V16: NCSA body type
  `ICFINALBODY` double DEFAULT NULL, -- V17: Final stage body class (for multi-stage vehicles)
  `GVWR_FROM` double DEFAULT NULL, -- V18: Power unit gross vehicle weight rating (from)
  `GVWR_TO` double DEFAULT NULL, -- V18: Power unit gross vehicle weight rating (to)
  `TOW_VEH` double DEFAULT NULL, -- V19: Vehicle trailing indicator (1 = Yes, 0 = No)
  
  `TRLR1VIN` text, -- V20: Trailer 1 VIN
  `TRLR2VIN` text, -- V20: Trailer 2 VIN
  `TRLR3VIN` text, -- V20: Trailer 3 VIN
  `TRLR1GVWR` double DEFAULT NULL, -- V21: Trailer 1 gross vehicle weight rating
  `TRLR2GVWR` double DEFAULT NULL, -- V21: Trailer 2 gross vehicle weight rating
  `TRLR3GVWR` double DEFAULT NULL, -- V21: Trailer 3 gross vehicle weight rating
  
  `J_KNIFE` double DEFAULT NULL, -- V22: Jackknife indicator (0 = Not an Articulated Vehicle, 1 = No, 2 = Yes-First Event, 3 = Yes-Subsequent Event)
  `MCARR_ID` text, -- V23: Motor carrier identification number (MCID)
  `MCARR_I1` double DEFAULT NULL, -- V23A: MCID issuing authority
  `MCARR_I2` text, -- V23B: MCID identification number
  `V_CONFIG` double DEFAULT NULL, -- V24: Vehicle configuration (e.g., single-unit, truck-trailer, tractor-trailer)
  `CARGO_BT` double DEFAULT NULL, -- V25: Cargo body type
  `HAZ_INV` double DEFAULT NULL, -- V26A: Hazardous materials involved (1 = No, 2 = Yes)
  `HAZ_PLAC` double DEFAULT NULL, -- V26B: Hazardous material placard present (0 = Not Applicable, 1 = No, 2 = Yes, 8 = Not Reported)
  `HAZ_ID` double DEFAULT NULL, -- V26C: Hazardous material identification number (0 = Not Applicable, 8888 = Not Reported)
  `HAZ_CNO` double DEFAULT NULL, -- V26D: Hazardous material class number (0 = Not Applicable, 88 = Not Reported)
  `HAZ_REL` double DEFAULT NULL, -- V26E: Release of hazardous material from cargo (0 = Not Applicable, 1 = No, 2 = Yes, 8 = Not Reported)
  `BUS_USE` double DEFAULT NULL, -- V27: Bus use (school, transit, charter, etc.)
  `SPEC_USE` double DEFAULT NULL, -- V28: Special use (e.g., taxi, rental, military)
  `EMER_USE` double DEFAULT NULL, -- V29: Emergency motor vehicle use (police, fire, ambulance)
  `TRAV_SP` double DEFAULT NULL, -- V30: Travel speed of the vehicle prior to crash (997 = Speed Greater than 151 mph, 998 = Not Reported, 999 = Unknown)
  `UNDEROVERRIDE` double DEFAULT NULL, -- V31: Vehicle underride/override relationship (0 = No Underride or Override, 1 = Underride, 2 = Override, 7 = Not Applicable, 8 = Not Reported, 9 = Reported as Unknown)
  `ROLLOVER` double DEFAULT NULL, -- V32: Rollover occurrence (0 = No, 1 = Yes)
  `ROLINLOC` double DEFAULT NULL, -- V33: Location of rollover (1 = On Roadway, 2 = On shoulder, 3 = On Median/Separator, 4 = In Gore, 5 = On Roadside, 6 = Outside of Trafficway, 7 = In Parking Lane/Zone, 8 = Not Applicable, 9 = Unknown)
  `IMPACT1` double DEFAULT NULL, -- V34A: Area of initial impact (0 = Non-Collision)
  `DEFORMED` double DEFAULT NULL, -- V35: Extent of vehicle damage (0 = No Damage, 2 = Minor Damage, 4 = Functional Damage, 6 = Disabling Damage, 8 = Not Reported, 9 = Unknown)
  `TOWAWAY` double DEFAULT NULL, -- V36: Duplicate Vehicle towed due to damage (5 = Not Towed, 6 = Towed, 8 = Not Reported, 9 = Unknown)
  `TOWED` double DEFAULT NULL, -- V36: Vehicle towed due to damage (5 = Not Towed, 6 = Towed, 8 = Not Reported, 9 = Unknown)
  `M_HARM` double DEFAULT NULL, -- V38: Most harmful event for this vehicle
  `FIRE_EXP` double DEFAULT NULL, -- V39: Fire occurrence (0 = No, 1 = Yes)
  `ADS_PRES` double DEFAULT NULL, -- V40A: Automation system present in vehicle (0 = No, 1 = Yes, 98 = Not Reported, 99 = Unknown)
  `ADS_LEV` double DEFAULT NULL, -- V40B: Highest automation level present (1-5 = Level of Automation Present, 9  = Automation Present Level Unknown, 98 = Not Reported, 99 = Unknown)
  `ADS_ENG` double DEFAULT NULL, -- V40C: Highest automation level engaged at time of crash (1-5 = Level of Automation Present, 6 = Automation Systems Engaged Level Unknown, 9  = Automation Present Level Unknown, 90 = Automation Present Not Engaged, 98 = Not Reported, 99 = Unknown)
  
  `MAK_MOD` bigint DEFAULT NULL, -- V100: NCSA combined make-model identifier
  
  -- VIN characters (individual positions from VIN)
  `VIN_1` text,  -- V101: VIN Character 1
  `VIN_2` text,  -- V102: VIN Character 2
  `VIN_3` text,  -- V103: VIN Character 3
  `VIN_4` text,  -- V104: VIN Character 4
  `VIN_5` text,  -- V105: VIN Character 5
  `VIN_6` text,  -- V106: VIN Character 6
  `VIN_7` text,  -- V107: VIN Character 7
  `VIN_8` text,  -- V108: VIN Character 8
  `VIN_9` text,  -- V109: VIN Character 9
  `VIN_10` text, -- V110: VIN Character 10
  `VIN_11` text, -- V111: VIN Character 11
  `VIN_12` text, -- V112: VIN Character 12
  
  `DEATHS` double DEFAULT NULL, -- V150: Fatalities in this vehicle
  `DR_DRINK` bigint DEFAULT NULL, -- V151: Driver drinking (1 = Yes, 0 = No)
  `DR_PRES` bigint DEFAULT NULL, -- D4: Driver presence (present, not present, unknown)
  `L_STATE` double DEFAULT NULL, -- D5: Driver's license state of issue
  `DR_ZIP` double DEFAULT NULL, -- D6: Driver’s ZIP code (00000 = Not a Resident of U.S. or Territories, 99997 = No Driver Present/Unknown if Driver Present, 99999 - Unknown)
  `L_TYPE` double DEFAULT NULL, -- D7A: Non-CDL license type (0 = Not Licensed, 1 = Full Driver License, 2 = Intermediate Driver License, 6 = No Driver Present/Unknown if Driver Present, 7 = Learner's Permit, 8 = Temporary License, 9 = Unknown License Type)
  `L_STATUS` double DEFAULT NULL, -- D7B: Non-CDL license status (valid, suspended, revoked, expired, cancelled)
  `CDL_STAT` double DEFAULT NULL, -- D8: Commercial driver license status (0 = No CDL, 1 = Suspended, 2 = Revoked, 3 = Expired, 4 = Cancelled or Denied, 5 = Disqualified, 6 = Valid, 7 = Commercial Leaner's Permit, 8 = Other - Not Valid, 99 = Unknown License Status)
  `L_ENDORS` double DEFAULT NULL, -- D9: Compliance with CDL endorsements
  `L_CL_VEH` double DEFAULT NULL, -- D10: License compliance with class of vehicle
  `L_COMPL` double DEFAULT NULL, -- D10: Duplicate Compliance with license restrictions
  `L_RESTRI` double DEFAULT NULL, -- D12: License restrictions (0 = No Restrictions or Not Applicable, 1 = Restrictions Complied With, 2 = Restrictions Not Complied With, 3 = Restrictions, Compliance Unknown, 9 = Unknown)
  `DR_HGT` double DEFAULT NULL, -- D13: Driver height (in inches) (999 = Unknown)
  `DR_WGT` double DEFAULT NULL, -- D14: Driver weight (in pounds) (999 = Unknown)
  `PREV_ACC` double DEFAULT NULL, -- D15A: Previous recorded crashes (99 = Unknown, 998 = No Driver Present/Unknown if Driver Present)
  `PREV_SUS1` double DEFAULT NULL, -- D15A/B/C: Previous administrative license suspensions (BAC) (99 = Unknown, 998 =  No Driver Present/Unknown if Driver Present)
  `PREV_SUS2` double DEFAULT NULL, -- D15B: Previous admin per se for BAC (not underage) (99 = Unknown, 998 =  No Driver Present/Unknown if Driver Present)
  `PREV_SUS3` double DEFAULT NULL, -- D15C: Previous other suspensions/revocations (99 = Unknown, 998 =  No Driver Present/Unknown if Driver Present)
  `PREV_DWI` double DEFAULT NULL, -- D16: Previous DWI convictions (99 = Unknown, 998 =  No Driver Present/Unknown if Driver Present)
  `PREV_SPD` double DEFAULT NULL, -- D17: Previous speeding convictions (99 = Unknown, 998 =  No Driver Present/Unknown if Driver Present)
  `PREV_OTH` double DEFAULT NULL, -- D18: Previous other moving violation convictions (99 = Unknown, 998 =  No Driver Present/Unknown if Driver Present)
  `FIRST_MO` double DEFAULT NULL, -- D19A: Month of oldest crash/suspension/conviction (0 = No Record, 1 = January, 2 = February, 3 = March, 4 = April, 5 = May, 6 = June, 7 = July, 8 = August, 9 = Septemeber, 10 = October, 11 = November, 12 = December, 99 = Unknown)
  `FIRST_YR` double DEFAULT NULL, -- D19B: Year of oldest crash/suspension/conviction (0 = No Record, 9999 = Unknown, 9998 =  No Driver Present/Unknown if Driver Present)
  `LAST_MO` double DEFAULT NULL, -- D20A: Month of most recent crash/suspension/conviction (0 = No Record, 1 = January, 2 = February, 3 = March, 4 = April, 5 = May, 6 = June, 7 = July, 8 = August, 9 = Septemeber, 10 = October, 11 = November, 12 = December, 99 = Unknown)
  `LAST_YR` double DEFAULT NULL, -- D20B: Year of most recent crash/suspension/conviction (0 = No Record, 9999 = Unknown, 9998 =  No Driver Present/Unknown if Driver Present)
  `SPEEDREL` double DEFAULT NULL, -- D22: Speeding related (0 = No, 2 = Yes - Racing, 3 = Yes - Exceeded Speed Limit, 4 = Yes - Too Fast for Conditions, 5 = Yes - Specifics Unknown, 8 = No Driver Present/Unknown if Driver Present, 9 = Unknown)
  
  `VTRAFWAY` double DEFAULT NULL, -- PC5: Trafficway description (0 = Non-Trafficway or Driveway Access, 1 = Two-Way Not Divided, 2 = Two-Way Divided Unprotected Median, 3 = Two Way Divided Positive Median Barrier, 4 = One-Way Trafficway, 5 = Two-Way Not Divided with a Continuous Left Turn Lane, 6 = Entrance/Exit Ramp, 7 = Two Way Divided Unknown if Unprotected Median or Positive Median Barrier, 8 = Not Reported, 9 = Unknown)
  `VNUM_LAN` double DEFAULT NULL, -- PC6: Total number of lanes in roadway (0 = Non-Trafficway or Driveway Access, 8 = Not Reported, 9 = Reported as Unknown)
  `VSPD_LIM` double DEFAULT NULL, -- PC7: Posted speed limit (98 = Not Reported, 99 = Unknown)
  `VALIGN` double DEFAULT NULL, -- PC8: Roadway alignment (0 = Non-Trafficway or Driveway Access, 1 = Straight, 2 = Curve Right, 3 = Curve Left, 4 = Curve - Unknown Direction, 8 = Not Reported, 9 = Unknown)
  `VPROFILE` double DEFAULT NULL, -- PC9: Roadway grade (0 = Non-Trafficway or Driveway Access, 1 = Level, 2 = Grade/Unknown Slope, 3 = Hillcrest, 4 = Sag (Bottom), 5 = Uphill, 6 = Downhill, 8 = Not Reported, 9 = Unknown)
  `VPAVETYP` double DEFAULT NULL, -- PC10: Roadway surface type (0 = Non-Trafficway or Driveway Access, 1 = Concrete, 2 = Blacktop/Bituminous/Asphalt, 3 = Brick/Block, 4 = Slag/Gravel/Stone, 5 = Dirt, 7 = Other, 8 = Not Reported, 9 = Unknown)
  `VSURCOND` double DEFAULT NULL, -- PC11: Roadway surface condition (0 = Non-Trafficway or Driveway Access, 1 = Dry, 2 = Wet, 3 = Snow, 4 = Ice/Frost, 5 = Sand, 6 = Water, 7 = Oil, 8 = Other, 10 = Slush, 11 = Mud/Dirt/Gravel, 98 = Not Reported, 99 = Unknown)
  `VTRAFCON` double DEFAULT NULL, -- PC12: Traffic control device (0 = No Controls, 1 =Traffic Control Signal (on Colors) Without Pedestrian Signal, 2 = Traffic Control Signal (on Colors) With Pedestrian Signal, 3 = Traffic Control Signal (on Colors) Not Known if Pedestrian Signal, 4 = Flashing Traffic Control Signal, 7 = Lane Use Control Signal, 8 = Other Highway Traffic Signal, 9 = Unknown Highway Traffic Signal, 20 = Stop Sign, 21 = Yield Sign, 28 = Other Regulatory Sign, 29 = Unknown Regulatory Sign, 23 = School Zone Sign/Device, 40 = Warning Sign, 50 = Person, 65 = Railway Crossing Device, 98 = Other, 99 = Unknown, 97 = Not Reported)  
  `VTCONT_F` double DEFAULT NULL, -- PC13: Traffic control device functioning (0 = No Controls, 1 = Device Not Functioning, 2 = Device Functioning – Functioning Improperly, 3 = Device Functioning Properly, 4 = Device Not Functioning or Device Functioning Improperly, Specifics Unknown, 8 = Not Reported, 9 = Unknown) 
  
  `P_CRASH1` double DEFAULT NULL, -- PC17: Pre-event movement (prior to recognition of critical event)
  `P_CRASH2` double DEFAULT NULL, -- PC19: Critical pre-crash event
  `P_CRASH3` double DEFAULT NULL, -- PC20: Attempted avoidance maneuver
  `PCRASH4` double DEFAULT NULL, -- PC21: Pre-impact stability (e.g., skidding)
  `PCRASH5` double DEFAULT NULL, -- PC22: Pre-impact location (e.g., on roadway, off roadway)
  `ACC_TYPE` double DEFAULT NULL, -- PC23: Crash type (e.g., single vehicle, multi-vehicle)
  `ACC_CONFIG` double DEFAULT NULL, -- PC23A: Crash type configuration (e.g., angle, rear-end, head-on)
  
  `YEAR` bigint DEFAULT NULL, -- Reporting year of the crash (matches accident_master.YEAR)
  
  PRIMARY KEY (`ST_CASE`)
);


-- =====================================================
-- Table: person_master
-- Description: Contains information about individuals involved in crashes.
-- Primary Key: ST_CASE (Crash Case Number)
-- Source: Fatality Analysis Reporting System (FARS)
-- =====================================================
CREATE TABLE `person_master` (
  `ST_CASE` bigint NOT NULL, -- Unique case number assigned to each crash record (Primary Key)

  `VEH_NO` bigint DEFAULT NULL, -- P1: Vehicle number associated with the person
  `PER_NO` bigint DEFAULT NULL, -- P2: Person number (unique within crash)

  `AGE` bigint DEFAULT NULL, -- P5/NM5: Age of person (in years) (998 = Not Reported, 999 = Unknown)
  `SEX` bigint DEFAULT NULL, -- P6/NM6: Sex of person (1 = Male, 2 = Female, 8 = Not Reported, 9 = Unknown)
  `PER_TYP` bigint DEFAULT NULL, -- P7/NM7: Person type (1 = Driver, 2 = Passenger, 3 = Occupant of Motor Vehicle In Transport, 4 = Occupant of a Non-Motor Vehicle Transport Device, 5 = Pedestrian, 6 = Bicyclist, 7 = Other Pedalcyclist, 8 = Person on Personal Conveyance, 9 = Unknown Occupant Type in a Motor Vehicle In-Transport, 10 = Person In/On a Building, 19 = Unknown Type of Non-Motorist)
  `INJ_SEV` bigint DEFAULT NULL, -- P8/NM8: Injury severity scale (KABCO scale) (0 = No Apparent Injury (O), 1 = Possible Injury (C), 2 = Suspected Minor Injury (B), 3 = Suspected Serious Injury (A), 4 = Fatal Injury (K), 5 = Injured, Severity Unknown, 6 = Died Prior to Crash, 9 = Unknown/Not Reported)
  `SEAT_POS` bigint DEFAULT NULL, -- P9: Seat position (e.g., driver seat, front passenger, rear seat)
  `REST_USE` bigint DEFAULT NULL, -- P10A: Restraint use (seat belt, harness, none)
  `REST_MIS` bigint DEFAULT NULL, -- P10B: Indication of restraint system misuse (0 = No Indication of Misuse, 1 = Yes Indication of Misuse, 7 = None Used/Not Applicable, 8 = Not a Motor Vehicle Occupant)
  `HELM_USE` bigint DEFAULT NULL, -- P11A: Helmet use (motorcyclist/bicyclist)
  `HELM_MIS` bigint DEFAULT NULL, -- P11B: Indication of helmet misuse (0 = No Indication of Misuse, 1 = Yes Indication of Misuse, 7 = None  Used/Not Applicable,  8 = Not a Motor Vehicle Occupant)
  `AIR_BAG` bigint DEFAULT NULL, -- P12: Air bag deployment status (1 = Deployed - Front, 2 = Deployed - Side (Door, Seat Back), 3 = Deployed - Curtain (Roof), 7 = Deployed - Other (Knee, Air Belt, etc.), 8 = Deployed - Combination, 9 = Deployment - Unknown Location, 20 = Not Deployed)
  `EJECTION` bigint DEFAULT NULL, -- P13: Ejection status (0 = Not Ejected, 1 = Totally Ejected, 2 = Partially Ejected, 3 = Ejected Unknown Degree, 7 = Not Reported, 8 = Not Applicable, 9 = Unknown)
  `EJ_PATH` bigint DEFAULT NULL, -- P14: Ejection path (0 = Ejection Path Not Applicable, 1 = Through Side Door Opening, 2 = Through Side Window, 3 = Through Windshield, 4 = Through Back Window, 5 = Through Back Door/Tailgate Opening, 6 = Through Roof Opening (Sun Roof, Convertible Top Down), 7 = Through Roof (Convertible Top Up), 8 = Other Path (e.g., Back of Pickup Truck), 9 = Ejection Path Unknown)
  `EXTRICAT` bigint DEFAULT NULL, -- P15: Extrication required (1 = Yes, 0 = No, 9 = Unknown)
  `DRINKING` bigint DEFAULT NULL, -- P16/NM18: Police Reported Alcohol Involvement (0 = No, 1 = Yes, 8 = Not Reported, 9 = Unknown)
  `ALC_STATUS` bigint DEFAULT NULL, -- P17A/NM19A: Alcohol test status (0 = Test Not Given, 2 = Test Given, 8 = Not Reported, 9 = Unknown)
  `ATST_TYP` bigint DEFAULT NULL, -- P17B/NM19B: Alcohol test type (0 = Not Tested for Alcohol, 1 = Blood Test, 2 = Breath Test (AC), 3 = Urine, 4 = Vitreous, 5 = Blood Plasma/Serum, 6 = Blood clot, 7 = Liver, 8 = Other Test Type, 10 = Preliminary Breath Test (PBT), 95 = Not Reported, 98 = Unknown Test Type, 99 = Unknown)
  `ALC_RES` bigint DEFAULT NULL, -- P17C/NM19C: Alcohol test result (BAC in g/dL)
  `DRUGS` bigint DEFAULT NULL, -- P18/NM20: Police Reported Drug Involvement (0 = No, 1 = Yes, 8 = Not Reported, 9 = Unknown)
  `DSTATUS` bigint DEFAULT NULL, -- P19A/NM21A: Drug test status (0 = Test Not Given, 2 = Test Given, 8 = Not Reported, 9 = Unknown)
  `HOSPITAL` bigint DEFAULT NULL, -- P20/NM22: Transported to hospital (0 = Not Transported, 1 = EMS Air, 2 = Law Enforcement, 3 = EMS Unknown Mode, 4 = Transported Unknown Source, 5 = EMS Ground, 6 = Other, 8 = Not Reported, 9 = Unknown)
  `DOA` bigint DEFAULT NULL, -- P21/NM23: Dead on arrival indicator (0 = Not Applicable, 7 = Died at Scene, 8 = Died En Route, 9 = Unknown)
  `DEATH_MO` bigint DEFAULT NULL, -- P22A/NM24A: Month of death (1 = January, 2 = February, 3 = March, 4 = April, 5 = May, 6 = June, 7 = July, 8 = August, 9 = September, 10 = October, 11 = November, 12 = December, 99 = Unknown) 
  `DEATH_DA` bigint DEFAULT NULL, -- P22B/NM24B: Day of death (if fatal) (0 = Not Applicable (Not Fatal), 88 = Not Applicable (Not Fatal), 99 = Unknown)
  `DEATH_YR` bigint DEFAULT NULL, -- P22C/NM24C: Year of death (0 = Not Applicable (Not Fatal), 8888 = Not Applicable (Not Fatal), 9999 = Unknown)
  `DEATH_TM` bigint DEFAULT NULL, -- P23/NM25: Death time (HHMM) (2400 = Midnight, 0 = Midnight, 8888 = Not Applicable (Not Fatal), 9999 = Unknown)
  `DEATH_HR` bigint DEFAULT NULL, -- P23A/NM25A: Hour of death (88 = Not Applicable, 99 = Unknown)
  `DEATH_MN` bigint DEFAULT NULL, -- P23B/NM25B: Minute of death (88 = Not Applicable, 99 = Unknown)
  `LAG_HRS` bigint DEFAULT NULL, -- P100A: Lag hours (time between crash and death) (99 = Unknown, 999 = Unknown)
  `LAG_MINS` bigint DEFAULT NULL, -- P100B: Lag minutes (time between crash and death) (99 = Unknown)
  `STR_VEH` bigint DEFAULT NULL, -- NM4: Number of Motor Vehicle Striking NonMotorist 
  `DEVTYPE` bigint DEFAULT NULL, -- NM8: Non-Motorist Device Type (0 = Not Applicable, 1 = Ridden Animal, Animal Drawn Conveyance, or Trailer, 2 = Railway Vehicle or Road Vehicle on Rails, 3 = Bicycle, 4 = Other Pedalcycle, 5 = Mobility Aid Device, 6 = Skates, 7 = Non-Self-Balancing Board (Skateboard), 8 = Self-Balancing Board, 9 = Standing or Seated Scooter, 97 = Personal Conveyance Other, 98 = Personal Conveyance Unknown Type, 99 = Unknown Type of Non-Motorist)
  `DEVMOTOR` bigint DEFAULT NULL, -- NM9: Non-Motorist Device Motorization (0 = Not Applicable, 1 = Not Motorized, 2 = Motorized, 3 = Unknown/Not Reported if Motorized, 9 = Unknown Type of Non-Motorist)
  `LOCATION` bigint DEFAULT NULL, -- NM12: Non-Motorist Location at Time of Crash
  `WORK_INJ` bigint DEFAULT NULL, -- SP2: Fatal Injury at Work (0 = No, 1 = Yes, 8 = Not Applicable, 9 = Unknown)
  `HISPANIC` bigint DEFAULT NULL, -- SP3B: Hispanic Origin (0 = Not a Fatality, 1 = Mexican, 2 = Puerto Rican, 3 = Cuban, 4 = Central or South American, 5 = European Spanish, 6 = Hispanic, Origin Not Specified or Other Origin, 7 = Non-Hispanic, 99 = Unknown)

  `YEAR` bigint DEFAULT NULL, -- Reporting year of the crash (matches accident_master.YEAR)

  PRIMARY KEY (`ST_CASE`)
);