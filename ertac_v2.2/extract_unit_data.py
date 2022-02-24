import sys
import sqlite3
import csv
import datetime

#conn = sqlite3.connect('../ga_1unit_case1/ertac2.db')
conn = sqlite3.connect('../sou_20120507a/ertac2.db')

fac_units = conn.execute("""SELECT orispl_code, unitid, facility_name, state, ertac_region, ertac_fuel_unit_type_bin
                         FROM unit_level_activity""").fetchall()

beg_2007 = datetime.datetime(2007,1,1,0,0,0)
beg_2008 = datetime.datetime(2008,1,1,0,0,0)
one_hour = datetime.timedelta(seconds=3600.)
yyyymmddhh_list = [beg_2007+i*one_hour for i in xrange(8760)]

output_header   = "op_date,op_hour,calendar_hour,hierarchy_hour,by_gl,fy_gl,by_hi,fy_hi,by_so2,fy_so2,by_nox,fy_nox".split(",")

#fac_units =[("991302","N13001","Plant Washington", "GA")]

for each_fac_unit in fac_units:
    orispl_code   = each_fac_unit[0]
    unitid        = each_fac_unit[1]
    facility_name = each_fac_unit[2]
    state         = each_fac_unit[3]
    print "Extracting State/Facility/Unit Data:", state, facility_name, unitid

    #print "    Building BY dataset..."
    by_data = conn.execute("""SELECT orispl_code, unitid, gload, heat_input, so2_mass, nox_mass, op_date, op_hour
    FROM calc_hourly_base
    WHERE orispl_code = ?
    AND unitid = ?""", (orispl_code, unitid)).fetchall()

    by_recs = {}
    if by_data == []:
        print "        No Actual BY data; therefore building fake dataset..."
        for yyyymmddhh in yyyymmddhh_list:
            by_recs[(orispl_code, unitid, yyyymmddhh)] = [None]*8
    else:
        for each_data in by_data:
            op_date     = each_data[6]
            op_hour     = each_data[7]
            yyyy        = int(op_date[:4])
            mm          = int(op_date[5:7])
            dd          = int(op_date[8:])
            hh          = int(op_hour)
            cur_date_hh = datetime.datetime(yyyy,mm,dd,hh)
            by_recs[(orispl_code, unitid, cur_date_hh)] = each_data

    #print "    Building FY dataset..."
    fy_data = conn.execute("""SELECT orispl_code, unitid, gload, heat_input, so2_mass, nox_mass, calendar_hour, hierarchy_hour
    FROM hourly_diagnostic_file
    WHERE orispl_code = ?
    AND unitid = ?""", (orispl_code, unitid)).fetchall()

    fy_recs = {}
    if fy_data == []:
        print "        No Actual FY data; therefore building fake dataset..."
        for yyyymmddhh in yyyymmddhh_list:
            fy_recs[(orispl_code, unitid, yyyymmddhh)] = [None]*8
    else:
        for each_data in fy_data:
            calendar_hour = int(each_data[6])
            cur_date_hh   = yyyymmddhh_list[calendar_hour-1]
            fy_recs[(orispl_code, unitid, cur_date_hh)] = each_data
        
    #print "    Combining BY & FY dataset..."
    output_line = [output_header]
    for yyyymmddhh in yyyymmddhh_list:
        #print "        ", str(yyyymmddhh)
        by_rec = by_recs[(orispl_code, unitid, yyyymmddhh)]
        fy_rec = fy_recs[(orispl_code, unitid, yyyymmddhh)]

        op_date        = str(yyyymmddhh)[:10]
        op_hour        = yyyymmddhh.hour
        calendar_hour  = int((yyyymmddhh - beg_2007).days*24.+(yyyymmddhh - beg_2007).seconds/3600.)+1
        hierarchy_hour = fy_rec[7]
        by_gl          = by_rec[2]
        fy_gl          = fy_rec[2]
        by_hi          = by_rec[3]
        fy_hi          = fy_rec[3]
        by_so2         = by_rec[4]
        fy_so2         = fy_rec[4]
        by_nox         = by_rec[5]
        fy_nox         = fy_rec[5]

        output_line.append([op_date,op_hour,calendar_hour,hierarchy_hour,by_gl,fy_gl,by_hi,fy_hi,by_so2,fy_so2,by_nox,fy_nox])

    output_filename = ("%s-%s-%s.csv" % (state, facility_name, unitid))
    out_file        = open(output_filename, "wb")
    out_file_writer = csv.writer(out_file)#, quoting=csv.QUOTE_NONE, escapechar = " ")
    out_file_writer.writerows(output_line)
    out_file.close()
