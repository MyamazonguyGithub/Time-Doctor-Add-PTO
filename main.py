import requests
import os
import pytz
import logging
import re
import calendar
from datetime import datetime, timedelta, date
from rate_limiter.python.time_doctor_throttler import TimeDoctorThrottler

time_doctor_throttler = TimeDoctorThrottler()

def get_users_with_pto():
    import requests

    AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN")
    BASE_ID = 'appfccXiah8EtMfbZ'
    TABLE_ID = 'tblD4HH6VsqXtBZ5G'
    VIEW_ID = 'viwvk8AEuzCy3Mjg5'

    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}"
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    params = {
        "view": VIEW_ID,
        "filterByFormula": "{Deducted PTO} != {TimeDoctor Total PTO Added}"
    }
    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    records = []

    for record in data.get("records", []):
        deducted_pto = record["fields"].get("Deducted PTO", 0)
        time_doctor_pto_added = record["fields"].get("TimeDoctor Total PTO Added", 0)
        available_pto = float(deducted_pto) - float(time_doctor_pto_added)
        asana_link = record["fields"].get("Asana Task Link", "")
        match = re.search(r'/project/(\d+)/task/(\d+)', asana_link)
        project_id = None
        task_id = None
        if match:
            project_id = match.group(1)
            task_id = match.group(2)
        else:
            logging.error("No project/task IDs found in Asana URL. record url: https://airtable.com/appfccXiah8EtMfbZ/tblD4HH6VsqXtBZ5G/" + record["id"])

        records.append({
            "id": record["id"],
            "td_logs": record["fields"].get("Time Doctor logs", []),
            "td_total_pto_added": record["fields"].get("TimeDoctor Total PTO Added", 0),
            "name": record["fields"].get("Name - Request Date", "").split(" - ")[0],
            "email": record["fields"].get("Email", [""])[0],
            "type": record["fields"].get("Request Type", ""),
            "available_pto": available_pto,
            "asana_project": project_id,
            "asana_task": task_id,
            'asana_link': asana_link
        })

    return {"records": records}

def search_workers(email):
    params = {
        "company": "YFpYQwOkUAAEWZlH",
        "filter[email]": email
    }
    url = "https://api2.timedoctor.com/api/1.0/users"
    resp = time_doctor_throttler.throttled_get(url, params=params)
    resp_json = resp.json()
    data = resp_json['data']
    try:
        return data[0]['id']
    except Exception as e:
        logging.error(f"Error while searching worker: {e}")
        return False

def get_td_time_log(user_id, date):
    date_str_y = date.strftime('%Y-%m-%d')
    date_str_t = (date + timedelta(days=1)).strftime('%Y-%m-%d')
    params = {
        "company": "YFpYQwOkUAAEWZlH",
        "user": user_id,
        "from": f'{date_str_y}T05:00:00Z',
        "to": f'{date_str_t}T04:59:59Z'
    }
    url = f"https://api2.timedoctor.com/api/1.0/activity/worklog"
    resp = time_doctor_throttler.throttled_get(url, params=params)
    resp_json = resp.json()
    data = resp_json['data'][0]

    return data

def add_timedoctor_pto(user_id, start_end_time, asana_link):
    TD_TOKEN = time_doctor_throttler.api_key
    headers = {
        "Authorization": f"JWT {TD_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    payload = {
        "userId": user_id,
        "start": start_end_time["start_time"],
        "end": start_end_time["end_time"],
        "taskId": 'Zgbg4e7SUiVONdHV',         # numeric ID
        "projectId": 'ZgRr0VuMLIiSdL-D',   # numeric ID
        "operation": "add",
        "reason": asana_link,
        "approved": True
    }
    try:
        resp = requests.post(
            "https://api2.timedoctor.com/api/1.0/activity/edit-time?company=YFpYQwOkUAAEWZlH",
            headers=headers,
            json=payload
        )

        resp.raise_for_status()
    except Exception as e:
        logging.error(f"Error while adding TimeDoctor PTO: {e}")
        return False
    
    return True

def get_start_and_end_time(start, end):
    est_tz = pytz.timezone('US/Eastern')

    # Convert UTC strings to EST datetime
    est_dt_s = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC).astimezone(est_tz)
    est_dt_e = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC).astimezone(est_tz)

    # Start and end of the day in EST
    start_of_day = est_tz.localize(datetime(est_dt_s.year, est_dt_s.month, est_dt_s.day, 0, 0, 0))
    end_of_day = est_tz.localize(datetime(est_dt_e.year, est_dt_e.month, est_dt_e.day, 23, 59, 59))

    four_hours = timedelta(hours=4)

    diff_s = (est_dt_s - start_of_day).total_seconds()
    diff_e = (end_of_day - est_dt_e).total_seconds()

    if diff_s >= 4 * 3600:
        # Interval is 4 hours ending at start
        interval_start_utc = (est_dt_s - four_hours).astimezone(pytz.UTC)
        interval_end_utc = est_dt_s.astimezone(pytz.UTC)
        start_str = interval_start_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_str = interval_end_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        return {"start_time": start_str, "end_time": end_str}

    if diff_e >= 4 * 3600: 
        # Interval is 4 hours starting from end
        interval_start_utc = est_dt_e.astimezone(pytz.UTC)
        interval_end_utc = (est_dt_e + four_hours).astimezone(pytz.UTC)
        start_str = interval_start_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_str = interval_end_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        return {"start_time": start_str, "end_time": end_str}
    
    return None


def post_asana_and_airtable(user=None, asana_comment=None, task_type="comment", airtable_logs=None, pto_added=None, close_ticket=False, status="success"):
    try:
        response = requests.post(
            "https://hooks.zapier.com/hooks/catch/24028680/uw7b946/",
            json={
                "status": status,
                "email": user['email'],
                "asana": {
                    "type": task_type,
                    "project_id": user['asana_project'],
                    "task_id": user['asana_task'],
                    "comment_text": asana_comment,
                    "close_task": close_ticket
                },
                "airtable": {
                    "recId": user['id'],
                    "pto_added": pto_added,
                    "airtable_logs": airtable_logs
                }
            }
        )
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Error while posting Asana comment: {e}")

def checkBusinessDay(action="check", today=datetime.today().date()):
    def last_monday_in_may(year):
        month_calendar = calendar.monthcalendar(year, 5)
        if month_calendar[-1][calendar.MONDAY] != 0:
            day = month_calendar[-1][calendar.MONDAY]
        else:
            day = month_calendar[-2][calendar.MONDAY]
        return date(year, 5, day)

    def first_monday_in_september(year):
        month_calendar = calendar.monthcalendar(year, 9)
        day = month_calendar[0][calendar.MONDAY]
        if day == 0: 
            day = month_calendar[1][calendar.MONDAY]
        return date(year, 9, day)

    def fourth_thursday_in_november(year):
        month_calendar = calendar.monthcalendar(year, 11)
        thursdays = [week[calendar.THURSDAY] for week in month_calendar if week[calendar.THURSDAY] != 0]
        day = thursdays[3] 
        return date(year, 11, day)
    
    def is_business_day(today):
        if (today.weekday() >= 5 or # Saturday=5, Sunday=6
        today.month == 1 and today.day == 1 or #new year
        today.month == 7 and today.day == 4 or #independence day
        today.month == 12 and today.day == 24 or #chrstmas eve
        today.month == 12 and today.day == 25 or #christmas
        last_monday_in_may(today.year) == today or #memorial day
        first_monday_in_september(today.year) == today or #labor day
        fourth_thursday_in_november(today.year) == today): #thanksgiving
            return False
        return True
    
    if action == "check":
        return is_business_day(today)
    
def main(devmode=False):
    est = pytz.timezone('US/Eastern')
    today_est = datetime.now(est).date()

    if devmode:
        yesterday_est = os.getenv("TEST_DATE", today_est.strftime('%Y-%m-%d'))
        yesterday_est = datetime.strptime(yesterday_est, '%Y-%m-%d').date()
        yesterday_est_str = yesterday_est.strftime('%Y-%m-%d')
    else:
        yesterday_est = today_est - timedelta(days=1) #1
        yesterday_est_str = yesterday_est.strftime('%Y-%m-%d')

    if checkBusinessDay(today=yesterday_est) == False:
        print(f"Yesterday {yesterday_est} is not a business day. Exiting...")
        return

    users_with_pto = get_users_with_pto()
    for user in users_with_pto["records"]:
        airtable_record_link = f"https://airtable.com/appfccXiah8EtMfbZ/tblD4HH6VsqXtBZ5G/{user['id']}"

        if user['email'] == '':
            logging.warning(f"No email found.\n\tUser {user['name']}\n\tAirtable Record: {airtable_record_link}")
            continue

        td_user = search_workers(user['email'])#"hannah.delacruz@myamazonguy.com")
        if not td_user: 
            logging.error(f"No TimeDoctor user found.\n\tEmail: {user['email']}\n\tAirtable Record: {airtable_record_link}")
            asana_comment = f"❌ Failed to add PTO Hours in TimeDoctor:\n\tReason: No TimeDoctor user found with email {user['email']}\n\t• PTO Date: {yesterday_est.strftime('%Y-%m-%d')}\n\t• Request Type: {user['type']}\n\t• Employee: {user['name']} ({user['email']})"
            airtable_logs = f"{user['td_logs']}, {yesterday_est_str}: ❌PTO not added in TD" if user['td_logs'] != [] else f"{yesterday_est_str}: ❌PTO not added in TD"
            post_asana_and_airtable(status="failed", user=user, asana_comment=asana_comment, task_type="comment,assign", airtable_logs=airtable_logs)
            continue

        td_time_log = get_td_time_log(td_user, yesterday_est)
        if len(td_time_log) > 0 and user['type'] not in ['Half Day Off', 'Flex-Time (Hours will be made up)']:
            logging.error(f"Failed to add PTO Hours in TD.\n\tEmail: {user['email']}\n\tAirtable Record: {airtable_record_link}")
            pto_to_add = max(0.5, min(user['available_pto'], 1)) if user['type'] not in ['Half Day Off', 'Flex-Time (Hours will be made up)'] else 0.5
            asana_comment = f"❌ Failed to add PTO Hours in TimeDoctor:\n\tReason: User has logged hours \n\t• PTO Date: {yesterday_est.strftime('%Y-%m-%d')}\n\t• Request Type: {user['type']}\n\t• PTO: {pto_to_add} ({'4 Hours' if pto_to_add == 0.5 else '8 Hours'})\n\t• Employee: {user['name']} ({user['email']})"
            airtable_logs = f"{user['td_logs']}, {yesterday_est_str}: ❌PTO not added in TD" if user['td_logs'] != [] else f"{yesterday_est_str}: ❌PTO not added in TD"
            post_asana_and_airtable(status="failed", user=user, asana_comment=asana_comment, task_type="comment,assign", airtable_logs=airtable_logs)
            continue
        
        if user['type'] == 'Half Day Off':
            if td_time_log == []:
                pto_to_add = 0.5
                start_str = f"{yesterday_est_str}T14:00:00.000Z"
                dt_utc = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)
                dt_utc_plus4 = dt_utc + timedelta(hours=4)
                utc_str_plus4 = dt_utc_plus4.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                start_end_time = {
                    "start_time": start_str,
                    "end_time": utc_str_plus4
                }
            else:
                td_log_start_time = td_time_log[0]['start']
                td_log_end_time = td_time_log[-1]['start']
                minutes_logged = td_time_log[-1]['time']/60
                td_log_end_dt_utc = datetime.strptime(td_log_end_time, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)
                td_log_end_dt_utc_plus = td_log_end_dt_utc + timedelta(minutes=minutes_logged)
                td_end_time = td_log_end_dt_utc_plus.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                start_end_time = get_start_and_end_time(td_log_start_time, td_end_time) #test only

        elif user['type'] not in ['Half Day Off', 'Flex-Time (Hours will be made up)']:
            pto_to_add = max(0.5, min(user['available_pto'], 1))
            start_str = f"{yesterday_est_str}T14:00:00.000Z"
            dt_utc = datetime.strptime(start_str, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)
            no_of_hrs = 8 if pto_to_add == 1 else 4
            dt_utc_plus8 = dt_utc + timedelta(hours=no_of_hrs)
            utc_str_plus8 = dt_utc_plus8.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            start_end_time = {
                "start_time": start_str,
                "end_time": utc_str_plus8
            }

        td_add_time = add_timedoctor_pto(td_user, start_end_time, user['asana_link'])
        #td_add_time = False #remove test only
        if td_add_time:
            print(f"Successfully added PTO Hours in TD.\n\tEmail: {user['email']}\n\tAirtable Record: {airtable_record_link}")
            asana_comment = f"✅ Successfully added PTO Hours in TimeDoctor:\n\t• PTO Date: {yesterday_est_str}\n\t• Request Type: {user['type']}\n\t• PTO: {pto_to_add} ({'4 Hours' if pto_to_add == 0.5 else '8 Hours'})\n\t• Employee: {user['name']} ({user['email']})"
            airtable_logs = f"{user['td_logs']}, {yesterday_est_str}: ✅PTO added in TD" if user['td_logs'] != [] else f"{yesterday_est_str}: ✅PTO added in TD"
            pto_added = float(user['td_total_pto_added']) + pto_to_add
            close_ticket = True if pto_to_add - user['available_pto'] <= 0 else False
            post_asana_and_airtable(user=user, asana_comment=asana_comment, task_type="comment", close_ticket=close_ticket, airtable_logs=airtable_logs,pto_added=pto_added)
        else:
            logging.error(f"Failed to add PTO Hours in TD.\n\tEmail: {user['email']}\n\tAirtable Record: {airtable_record_link}")
            asana_comment = f"❌ Failed to add PTO Hours in TimeDoctor:\n\t• PTO Date: {yesterday_est_str}\n\t• Request Type: {user['type']}\n\t• PTO: {pto_to_add} ({'4 Hours' if pto_to_add == 0.5 else '8 Hours'})\n\t• Employee: {user['name']} ({user['email']})"
            airtable_logs = f"{user['td_logs']}, {yesterday_est_str}: ❌PTO not added in TD" if user['td_logs'] != [] else f"{yesterday_est_str}: ❌PTO not added in TD"
            post_asana_and_airtable(status="failed", user=user, asana_comment=asana_comment, task_type="comment,assign", airtable_logs=airtable_logs)


if __name__ == "__main__":
    devmode = os.getenv("DEV_MODE", "false").lower() == "true"
    print("--- Starting script execution ---")
    main(devmode=devmode)
    print("--- Script execution completed ---")