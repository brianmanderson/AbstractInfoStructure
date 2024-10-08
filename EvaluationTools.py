from .AbstractBase import *
try:
    from tqdm import tqdm
except:
    tqdm = None
try:
    from threading import Thread
    from queue import *
    from multiprocessing import cpu_count
except ImportError:
    print("Unable to potentially load data in parallel")
    load_parallel = False
try:
    import shutil
except ImportError:
    print("Unable to import shuttle, cannot update local database")


def find_all_rois(header_databases: PatientHeaderDatabases):
    """
    :param header_databases:
    :return:
    """
    all_rois = []
    for header_database in header_databases.HeaderDatabases.values():
        for pat in header_database.PatientHeaders.values():
            for case in pat.Cases:
                for roi in case.ROIS:
                    if roi.Name.lower() not in all_rois:
                        all_rois.append(roi.Name.lower())
    """
    Remove PTV contours, opts, tuning structures, and DNU
    """
    reduced_all_rois = [i for i in all_rois if i.find('ptv') == -1 and i.find('opt') == -1 and i.find('hot') == -1 and
                        i.find('cold') == -1 and i.find('avo') == -1 and i.find('norm') == -1 and i.find('tune') == -1
                        and len(i) > 2 and i.find('ring') == -1 and i.find('couch') == -1 and i.find('max') == -1
                        and i.find('min') == -1 and i.find('push') == -1 and i.find('shell') == -1
                        and i.find('warm') == -1 and i.find('avd') == -1 and i.find('dnu') == -1
                        and i.find('notused') == -1]
    return reduced_all_rois


def update_database(network_path, local_path):
    today = DateTimeClass()
    today.from_python_datetime(datetime.today())
    last_update_date = os.path.join(local_path, "Last_Updated.txt")
    if not os.path.exists(last_update_date):
        update_local_database(local_database_path=local_path,
                              network_database_path=network_path, tqdm=tqdm)
    else:
        last_update = DateTimeClass()
        fid = open(last_update_date)
        dates = fid.readline().split('.')
        fid.close()
        last_update.year = int(dates[0])
        last_update.month = int(dates[1])
        last_update.day = int(dates[2])
        if (today - last_update).days >= 1:
            update_local_database(local_database_path=local_path,
                                  network_database_path=network_path, tqdm=tqdm)


def identify_wanted_headers(patient_header_dbs: PatientHeaderDatabases,
                            wanted_roi_list: List[str], wanted_type: List[str]):
    out_header_dbs = PatientHeaderDatabases()
    wanted_type = [i.lower() for i in wanted_type]
    for pat_header_db in patient_header_dbs.HeaderDatabases.values():
        out_header_db = PatientHeaderDatabase(pat_header_db.DBName)
        for pat in pat_header_db.PatientHeaders.values():
            has_roi = False
            for case in pat.Cases:
                wanted_rois = [r for r in case.ROIS if r.Name.lower() in wanted_roi_list and
                               r.Type.lower() in wanted_type]
                if wanted_rois:
                    has_roi = True
            if has_roi:
                out_header_db.PatientHeaders[pat.RS_UID] = pat
        out_header_dbs.HeaderDatabases[out_header_db.DBName] = out_header_db
    return out_header_dbs


def copy_file(a):
    q: Queue
    q, pbar = a
    update = False
    if pbar is not None:
        update = True
    while True:
        files_tuple = q.get()
        if files_tuple is None:
            break
        db_file_path, local_file_path = files_tuple
        if update:
            pbar.update()
        shutil.copy(db_file_path, local_file_path)
        q.task_done()


def check_case_has_approved(case: CaseClass or StrippedDownCase):
    for tp in case.TreatmentPlans:
        if check_is_plan_approved(tp):
            return True
    return False


def check_patient_has_approved(patient: PatientClass or PatientHeader):
    for case in patient.Cases:
        if check_case_has_approved(case):
            return True
    return False


def check_is_plan_approved(tp: StrippedDownPlan or TreatmentPlanClass):
    if tp.Review is None:
        return False
    review: ReviewClass
    review = tp.Review
    if review.ApprovalStatus == "Approved":
        return True
    return False


def return_plan_names_by_contains(patients: List[PatientHeader] or List[PatientClass], find_str: str):
    plan_names: List[str]
    plan_names = []
    for patient in patients:
        for case in patient.Cases:
            for plan in case.TreatmentPlans:
                is_approved = check_is_plan_approved(plan)
                if not is_approved:
                    if plan.PlanName.lower().find(find_str) != -1:
                        if plan.PlanName not in plan_names:
                            plan_names.append(plan.PlanName)
    return plan_names


def update_local_database(local_database_path: Union[str, bytes, os.PathLike],
                          network_database_path: Union[str, bytes, os.PathLike], tqdm=None):
    try:
        import shutil
    except:
        print("Could not import shutil!")
        return
    databases = []
    for root, databases, files in os.walk(network_database_path):
        break

    copy_files = []
    for database in databases:
        print(f"Updating {database}")
        db_path = os.path.join(network_database_path, database)
        local_db_path = os.path.join(local_database_path, database)
        if not os.path.exists(local_db_path):
            os.makedirs(local_db_path)
        db_files = os.listdir(db_path)
        db_json_files = [i for i in db_files if i.endswith('.json')]
        local_files = os.listdir(local_db_path)
        local_json_files = [i for i in local_files if i.endswith('.json')]
        for db_json_file in db_json_files:
            """
            If we do not have an instance of this file locally, copy it over
            """
            if db_json_file not in local_json_files:
                db_file_path = os.path.join(db_path, db_json_file)
                local_file_path = os.path.join(local_db_path, db_json_file)
                copy_files.append((db_file_path, local_file_path))
        for local_json_file in local_json_files:
            """
            If this file does no exist in the database, delete it
            """
            if local_json_file not in db_json_files:
                os.remove(os.path.join(local_db_path, local_json_file))
    if load_parallel:
        pbar = None
        if tqdm is not None:
            pbar = tqdm(total=len(copy_files), desc='Adding patients from network databases')
        if load_parallel:
            thread_count = int(cpu_count() * 0.8 - 1)
            q = Queue(maxsize=thread_count)
            threads = []
            a = (q, pbar)
            for worker in range(thread_count):
                t = Thread(target=copy_file, args=(a,))
                t.start()
                threads.append(t)
            for file_copy in copy_files:
                q.put(file_copy)
            for _ in range(thread_count):
                q.put(None)
            for t in threads:
                t.join()
    else:
        for file_copy in copy_files:
            db_file_path, local_file_path = file_copy
            shutil.copy(db_file_path, local_file_path)
    today = DateTimeClass()
    today.from_python_datetime(datetime.today())
    last_update_date = os.path.join(local_database_path, "Last_Updated.txt")
    fid = open(last_update_date, 'w+')
    fid.write(f"{today.year}.{today.month}.{today.day}.{today.hour}")
    fid.close()


def return_approved_db(database: PatientHeaderDatabase or PatientDatabase):
    if isinstance(database, PatientDatabase):
        database: PatientDatabase
        out_database = PatientDatabase(database.DBName)
        for patient in database.Patients.values():
            if check_patient_has_approved(patient):
                out_database.Patients[patient.RS_UID] = patient
    else:
        database: PatientHeaderDatabase
        out_database = PatientHeaderDatabase(database.DBName)
        for patient in database.PatientHeaders.values():
            if check_patient_has_approved(patient):
                out_database.PatientHeaders[patient.RS_UID] = patient
    return out_database


def return_databases_by_mrn(databases: PatientDatabases, mrn_list: List[str]):
    out_database = PatientDatabases()
    for db in databases.Databases.values():
        reduced_db = PatientDatabase(db.DBName)
        for pat in db.Patients.values():
            if pat.MRN in mrn_list:
                reduced_db.Patients[pat.RS_UID] = pat
        if len(reduced_db.Patients) != 0:
            out_database[reduced_db.DBName] = reduced_db
    return out_database


def __return_roi_list__(patient_db: PatientDatabase, rois: List[str] = None) -> List[RegionOfInterestBase]:
    if rois is None:
        rois = []
    for patient in patient_db.Patients.values():
        for case in patient.Cases:
            for roi in case.Base_ROIs:
                if roi.Name not in rois:
                    rois.append(roi)
    return rois


def return_roi_list_from_db(patient_db: PatientDatabases or PatientDatabase,
                            rois: List[RegionOfInterestBase] or None = None):
    if rois is None:
        rois = []
    if type(patient_db) is PatientDatabases:
        patient_db: PatientDatabases
        for db in patient_db.Databases.values():
            rois += __return_roi_list__(db)
    else:
        rois = __return_roi_list__(patient_db)
    return rois


if __name__ == '__main__':
    pass
