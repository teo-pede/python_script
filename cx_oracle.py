import cx_Oracle
import os
import csv
import datetime
import smtplib
import datetime
import time
import logging
import email
import threading
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as dates
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pandas.plotting import register_matplotlib_converters


def handleError(error_message, logger, env):
    logger.error(error_message+' from env: '+env)

def sendMail(subject, text, html, filename, env, loggerE):
    sender_email = "xxxxxxxxxxxxxxxxxxxxxxxxxx"
    receiver_email = ["wwwwwwwwwwwwwwwwwwwwwwwwwww","zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"]
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"
    message["To"] = "xxxxxxxxxxxxxxxxxxxxxxxxxx"

    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    message.attach(part1)
    message.attach(part2)

    if filename is not None:
        with open(filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        name=filename.split('\\')[-1]
        part.add_header(
            "Content-Disposition",
            f"attachment; filename= {name}",
        )
        message.attach(part)

    try:
        smtpObj = smtplib.SMTP('ip_SMTP_server')
        smtpObj.sendmail(sender_email, receiver_email, message.as_string())
    except SMTPException:
        handleError ("Error: unable to send email", loggerE, env)

def make_fig(env,data):
    if os.path.exists("path_to_file/image"):
        os.remove("path_to_file/image")
    tmp_data=data[data["environment"]==env].reset_index()
    fig, ax = plt.subplots()
    ax.plot_date(tmp_data["time"],tmp_data["total_bytes_mb"], '-')
    ax.xaxis.set_minor_locator(dates.HourLocator(interval=1)) 
    plt.ylabel("MB")
    ax.get_yaxis().get_major_formatter().set_useOffset(False)
    fig.set_size_inches((0.3*len(tmp_data["time"].unique())), (0.8*len(tmp_data["total_bytes_mb"].unique())))
    ax.xaxis.set_minor_formatter(dates.DateFormatter('%H:%M'))
    for min_tick in ax.get_xminorticklabels():
        min_tick.set_rotation(45)
        min_tick.set_size(7)
    ax.xaxis.set_major_locator(dates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(dates.DateFormatter('\n%d-%m-%Y')) 
    for maj_tick in ax.get_xmajorticklabels():
        maj_tick.set_rotation(90)
        maj_tick.set_size(10)
    plt.savefig("path_to_file/image",dpi=600,bbox_inches='tight',pad_inches=0.3)

def main():
    loggerE = logging.getLogger('error')
    loggerE.setLevel(logging.ERROR)
    error = logging.FileHandler('path_to_error_log_file')
    error.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    error.setFormatter(formatter)
    loggerE.addHandler(error)
    

    param = {
        #dict with adress , user and psw to connect to DB
    }

    sql_mb_free="""
    ##any query you want to make. eg:
    SELECT ts.tablespace_name,SUM(NVL(fs.bytes,0))/1024/1024 AS MB_FREE FROM user_free_space fs,user_tablespaces ts WHERE fs.tablespace_name(+) = ts.tablespace_name GROUP BY ts.tablespace_name

    SELECT case when lb.column_name is not null then lb.table_name||'.'||lb.column_name||' ('||sg.segment_name||')' else sg.segment_name end as segment_name, sg.tablespace_name,sg.segment_type, sum(sg.bytes)/1024/1024 as total_bytes_mb FROM user_segments sg left join user_lobs lb on lb.segment_name = sg.segment_name GROUP BY sg.segment_name, sg.segment_type, lb.column_name, .table_name, sg.tablespace_name ORDER BY total_bytes_mb desc"""

    while True:

        write_rows=[]
        cur_time = datetime.datetime.now().time()
        cur_date = np.datetime64('now')
        cur_day = datetime.date.today()
        data = None


        for #browse dict to get connection info
            try:
                connection = cx_Oracle.connect("user", param[env]["psw"], param[env]["server"], encoding="UTF-8")
                cursor = connection.cursor()

                sql_occ_arr=[]

                for row in cursor.execute(sql_occupancy):
                    tmp=[str(cur_date)]
                    sql_occ_tmp=[]
                    for att in row:
                        tmp+=[att]
                        sql_occ_tmp+=[att]
                    write_rows+=[tmp]
                    sql_occ_arr+=[sql_occ_tmp]

                text="some test in plain text\n\n"
                html="same as text var but in html markup"

                for row in sql_occ_arr:
                    html+="<tr>"
                    for att in row:
                        html+="<td>"+str(att)+"</td>"
                        text+=str(att)+"\t"
                    html+="</tr>"
                    text+="\n"


                thread = threading.Thread(target=make_fig(env,data))
                thread.start()
                thread.join()
                sendMail("subjet", text, html, "path_to_img_file", env,loggerE)


                cursor.close()
                connection.close()

            except:
                handleError("ERROR - problem quering DB",loggerE,env)

        with open("path_to_archive_rsults", 'a',newline='') as csvFileOut:
            writer = csv.writer(csvFileOut, delimiter=';')
            for r in write_rows:
                writer.writerow(r)
        csvFileOut.close()

        time.sleep(3600)


if __name__ == "__main__":
    main()