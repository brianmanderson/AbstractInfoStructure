from typing import List, Dict, Union
import os
import json
from datetime import datetime
load_parallel = True
try:
    from threading import Thread
    from queue import *
    from multiprocessing import cpu_count
except ImportError:
    print("Unable to potentially load data in parallel")
    load_parallel = False


def compare_dicts(dict1, dict2):
    # Check if both arguments are dictionaries
    if not isinstance(dict1, dict) or not isinstance(dict2, dict):
        return dict1 == dict2

    # Check if the keys are the same in both dictionaries
    if set(dict1.keys()) != set(dict2.keys()):
        print(dict1.keys())
        return False

    # Recursively compare values for each key
    for key in dict1.keys():
        if not compare_values(dict1[key], dict2[key]):
            """
            This might look weird, but 'nan' does not equal itself
            We are now checking to see if both values are 'nan', if so, great!
            """
            if dict1[key] == dict1[key] or dict2[key] == dict2[key]:
                return False

    return True


def compare_values(value1, value2):
    if isinstance(value1, list) and isinstance(value2, list):
        if len(value1) != len(value2):
            print(value1)
            return False
        for v1, v2 in zip(value1, value2):
            if not compare_values(v1, v2):
                return False
        return True
    elif isinstance(value1, dict) and isinstance(value2, dict):
        return compare_dicts(value1, value2)
    elif hasattr(value1, '__dict__') and hasattr(value2, '__dict__'):
        return compare_dicts(value1.__dict__, value2.__dict__)
    else:
        return value1 == value2


def add_patient(a):
    patient_dictionary: PatientDatabase.Patients
    q: Queue
    q, pbar, patient_dictionary = a
    update = False
    if pbar is not None:
        update = True
    while True:
        file = q.get()
        if file is None:
            break
        if update:
            pbar.update()
        patient = PatientClass.from_json_file(file)
        patient_dictionary[patient.RS_UID] = patient
        q.task_done()


def add_patient_header(a):
    patient_header_dictionary: PatientHeaderDatabase.PatientHeaders
    q: Queue
    q, pbar, patient_header_dictionary = a
    update = False
    if pbar is not None:
        update = True
    while True:
        file = q.get()
        if file is None:
            break
        if update:
            pbar.update()
        patient_header = PatientHeader.from_json_file(file)
        patient_header.FilePath = file
        patient_header_dictionary[patient_header.RS_UID] = patient_header
        q.task_done()


class BaseMethod:

    def build(self, *args, **kwargs):
        pass

    def define_uid(self, uid: int):
        pass

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def to_json_file(self, json_file_path):
        json_dict = self.to_json()
        with open(json_file_path, 'w') as json_file:
            json_file.write(json_dict)

    def to_json(self):
        json_dict = {'__' + self.__class__.__name__ + '__': True}
        for attribute, attribute_type in self.__annotations__.items():
            if not hasattr(self, attribute):
                continue
            attribute_value = getattr(self, attribute)
            # Check if attribute is a list
            if isinstance(attribute_value, list):
                out_list = []
                for element in attribute_value:
                    if hasattr(element, 'to_json'):
                        out_list.append(element.to_json())
                    else:
                        out_list.append(element)
                json_dict[attribute] = out_list

            # Check if attribute is a dictionary
            elif isinstance(attribute_value, dict):
                out_dict = {}
                for key, value in attribute_value.items():
                    if hasattr(key, 'to_json'):
                        key = key.to_json()
                    if hasattr(value, 'to_json'):
                        value = value.to_json()
                    out_dict[key] = value
                json_dict[attribute] = out_dict

            # Handle other types
            else:
                if hasattr(attribute_value, "to_json"):
                    json_dict[attribute] = attribute_value.to_json()
                else:
                    json_dict[attribute] = attribute_value

        return json.dumps(json_dict)

    @classmethod
    def from_json_file(cls, json_file_path):
        with open(json_file_path, 'r') as json_file:
            json_str = json_file.readlines()[0]
        return cls().from_json(json_str)

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        if '__' + cls.__name__ + '__' in data:
            temp = cls()
            for attribute, attribute_type in temp.__annotations__.items():
                if attribute in data:
                    if hasattr(attribute_type, "__origin__"):
                        if attribute_type.__origin__ == list or attribute_type.__origin__ is List:
                            sub_type = attribute_type.__args__[0]
                            if hasattr(sub_type, "from_json"):
                                setattr(temp, attribute, [sub_type.from_json(i) for i in data[attribute]])
                            else:
                                setattr(temp, attribute, data[attribute])
                        elif attribute_type.__origin__ == dict or attribute_type.__origin__ is Dict:
                            key_type = attribute_type.__args__[0]
                            value_type = attribute_type.__args__[1]
                            for key, value in data[attribute].items():
                                if hasattr(key_type, "from_json"):
                                    key_value = key_type.from_json(key)
                                else:
                                    key_value = key_type(key)
                                if hasattr(value_type, "from_json"):
                                    value_value = value_type.from_json(value)
                                else:
                                    value_value = value_type(value)
                                getattr(temp, attribute).update({key_value: value_value})
                    elif hasattr(attribute_type, "from_json"):
                        if data[attribute] is not None:
                            setattr(temp, attribute, attribute_type.from_json(data[attribute]))
                        else:
                            setattr(temp, attribute, data[attribute])
                    else:
                        setattr(temp, attribute, data[attribute])
            return temp
        else:
            raise ValueError("JSON")


class DateTimeClass(BaseMethod):
    year: int
    month: int
    day: int
    hour: int
    minute: int
    second: int

    def __init__(self):
        self.year = 0
        self.day = 1
        self.month = 1
        self.hour = 0
        self.minute = 0
        self.second = 0

    def __eq__(self, other):
        if isinstance(other, DateTimeClass):
            if self.__dict__ == other.__dict__:
                return True
        return False

    def __sub__(self, other):
        assert isinstance(other, DateTimeClass)
        k = datetime(self.year, self.month, self.day, self.hour, self.minute, self.second)
        k2 = datetime(other.year, other.month, other.day, other.hour, other.minute, other.second)
        return k - k2

    def from_rs_datetime(self, k):
        self.year = k.Year
        self.month = k.Month
        self.day = k.Day
        self.hour = k.Hour
        self.minute = k.Minute
        self.second = k.Second

    def from_python_datetime(self, k: datetime):
        self.year = k.year
        self.month = k.month
        self.day = k.day
        self.hour = k.hour
        self.minute = k.minute
        self.second = k.second

    def from_pandas_timestamp(self, k):
        self.from_python_datetime(k)

    def from_string(self, k):
        year, month, day, hour, minute = k.split('.')
        self.year = int(year)
        self.month = int(month)
        self.day = int(day)
        self.hour = int(hour)
        self.minute = int(minute)

    def __repr__(self):
        return str(self.month) + '/' + str(self.day) + '/' + str(self.year)


class MultipleDateTimes(BaseMethod):
    Number_list: List[int]
    Number_dict: Dict[str, int]
    DateTimeList: List[DateTimeClass]
    DateTimeDict: Dict[int, DateTimeClass]
    Number: int
    String: str

    def __init__(self):
        self.DateTimeList = []
        self.Number_dict = {}
        self.DateTimeDict = {}


class RoiMaterial(BaseMethod):
    Name: str
    MassDensity: float

    def __repr__(self):
        return self.Name


class OrganDataClass(BaseMethod):
    OrganType: str  # Can be Organ Type or Code Meaning (0008, 0104)
    CodeValue: str  # Meant to be (0008, 01000)
    CodeSchemeDesignator: str  # Meant to be (0008, 0102)
    ResponseFunctionTissueName: str

    def __repr__(self):
        return self.ResponseFunctionTissueName


class RegionOfInterestBase(BaseMethod):
    Name: str
    RS_Number: int  # A Raystation integer unique within the treatment case
    Type: str
    Base_ROI_UID: int = 0
    ROI_Material: RoiMaterial or None
    OrganData: OrganDataClass or None
    StructureCode: str or None

    def __repr__(self):
        return self.Name


class PointOfInterestBase(BaseMethod):
    Name: str
    RS_Number: int  # A Raystation integer unique within the treatment case
    Type: str
    Base_POI_UID: int = 0
    OrganType: str or None
    ROI_Material: RoiMaterial or None
    OrganData: OrganDataClass or None

    def __repr__(self):
        return self.Name


class PointOfInterest(BaseMethod):
    Name: str
    RS_Number: int  # A Raystation integer unique within the treatment case
    Defined: bool
    POI_UID: int = 0
    x: float
    y: float
    z: float

    def __repr__(self):
        return self.Name


class RegionOfInterest(BaseMethod):
    Name: str
    RS_Number: int  # A Raystation integer unique within the treatment case
    ROI_UID: int = 0
    Volume: float
    HU_Min: float
    HU_Max: float
    HU_Average: float
    Defined: bool

    def __repr__(self):
        return self.Name


class EquipmentInfoClass(BaseMethod):
    FrameOfReference: str
    Modality: str


class ExaminationClass(BaseMethod):
    Exam_UID: int = 0
    EquipmentInfo: EquipmentInfoClass or None
    ROIs: List[RegionOfInterest]
    POIs: List[PointOfInterest]
    ExamName: str
    SeriesDescription: str
    SeriesInstanceUID: str
    StudyInstanceUID: str
    StudyDescription: str
    Exam_DateTime: DateTimeClass or None

    def __init__(self):
        self.ROIs = []
        self.POIs = []
        self.EquipmentInfo = None
        self.Exam_DateTime = None

    def __repr__(self):
        return self.ExamName


class RegionOfInterestDose(BaseMethod):
    AbsoluteDose: List[float]  # DVH will be the dose at relative volume from 1-100%
    RelativeVolumes: List[float]  # DVH volumes won't be exactly 0-100%, these are picked up from the voxels
    Dose_Min_cGy: float = 0.0
    Dose_Max_cGy: float = 0.0
    Dose_Average_cGy: float = 0.0
    Dose_ROI_UID: int = 0
    RS_Number: int
    Name: str
    ScalingFactor: int = 1
    Defined: bool = False
    dvh_step: float = 0.01
    AttemptedUpdate: bool = False

    def __repr__(self):
        return self.Name


class PointOfInterestDose(BaseMethod):
    Dose_cGy: float
    Name: str
    Dose_POI_UID: int = 0
    RS_Number: int
    ScalingFactor: int = 1

    def __repr__(self):
        return self.Name


class DoseSpecificationPointClass(BaseMethod):
    x: float
    y: float
    z: float
    Name: str

    def __repr__(self):
        return self.Name


class PrescriptionClass(BaseMethod):
    Prescription_UID: int = 0
    DoseAbsoluteVolume_cc: float
    DoseValue_cGy: float
    DoseVolume_percent: float
    RelativePrescriptionLevel: float
    PrescriptionType: str
    Referenced_ROI_Structure: RegionOfInterest or None
    Referenced_POI_Structure: PointOfInterest or None
    DoseSpecificationPoint: DoseSpecificationPointClass or None
    NumberOfFractions: int
    Dose_per_Fraction: float


class BeamClass(BaseMethod):
    ArcRotationDirection: str or None
    ArcStopGantryAngle: float
    CollimatorAngle: float
    BeamMU: float
    BeamQualityId: str  # Typically energy ' ' qualifier (FFF, SRS)
    CouchRotationAngle: float
    DeliveryTechnique: str
    Description: str
    GantryAngle: float
    PlanGenerationTechnique: str
    BeamName: str
    RS_BeamNumber: int
    BeamNumber_UID: int = 0
    SSD: float = -1.0  # Defaults to -1, can only be defined if an external ROI is present

    def __repr__(self):
        return self.Description


class MachineReferenceClass(BaseMethod):
    MachineName: str
    CommissioningTime: DateTimeClass or None

    def __repr__(self):
        return self.MachineName


class FractionDoseClass(BaseMethod):
    Name: str
    FractionDose_UID: int = 0
    DoseROIs: List[RegionOfInterestDose]
    DosePOIs: List[PointOfInterestDose]

    def __init__(self):
        self.DosePOIs = []
        self.DoseROIs = []

    def __repr__(self):
        return self.Name


class BeamSetClass(BaseMethod):
    NumberOfFractions: int = 1
    RS_BeamNumber: int  # The number of beam held in RS, starts in each plan
    BeamSetUID: int = 0
    DicomPlanLabel: str
    Prescriptions: List[PrescriptionClass]
    Primary_Prescription: PrescriptionClass or None
    Primary_Prescription_UID: int
    PlanIntent: str
    PlanGenerationTechnique: str
    Modality: str
    Beams: List[BeamClass]
    MachineReference: MachineReferenceClass or None
    FractionDose: FractionDoseClass or None

    def __init__(self):
        self.Prescriptions = []
        self.Beams = []

    def __repr__(self):
        return self.DicomPlanLabel


class PlanOptimizationClass(BaseMethod):
    AutoScaleToPrescription: bool
    Referenced_BeamSetsNames: List[str]
    Optimizer_UID: int = 0

    def __init__(self):
        self.Referenced_BeamSetsNames = []


class ReviewClass(BaseMethod):
    ApprovalStatus: str  # Approval status
    ReviewerName: str  # The credentials of the reviewer
    ReviewTime: DateTimeClass  # The datetime when the review was performed

    def __repr__(self):
        return self.ApprovalStatus


class TreatmentPlanClass(BaseMethod):
    PlanName: str
    PlannedBy: str or None
    TreatmentPlan_UID: int = 0
    FractionNumber: int  # 0 indicates pre-treatment
    BeamSets: List[BeamSetClass]
    Optimizations: List[PlanOptimizationClass]
    Referenced_Exam_Name: str
    Review: ReviewClass or None

    def __repr__(self):
        return self.PlanName


class RegistrationClass(BaseMethod):
    Registration_UID: int = 0
    IsDeformable: bool
    FromFrameOfReference: str
    ToFrameOfReference: str
    RigidTransformMatrix: List[float]
    StructureRegistrationsNames: List[str]


class CaseClass(BaseMethod):
    CaseName: str
    Case_UID: int = 0
    BodySite: str
    HadDeformableReg: bool
    Base_ROIs: List[RegionOfInterestBase]
    Base_POIs: List[PointOfInterestBase]
    Examinations: List[ExaminationClass]
    TreatmentPlans: List[TreatmentPlanClass]
    Registrations: List[RegistrationClass]

    def __init__(self):
        self.Examinations = []
        self.TreatmentPlans = []
        self.Registrations = []
        self.Base_ROIs = []
        self.Base_POIs = []
        self.HadDeformableReg = False

    def delete_unapproved_plans(self):
        for tp in self.TreatmentPlans[:]:
            if tp.Review is None:
                self.TreatmentPlans.remove(tp)
            else:
                review: ReviewClass
                review = tp.Review
                if review.ApprovalStatus != "Approved":
                    self.TreatmentPlans.remove(tp)

    def add_all_treatment_plans(self, rs_case):
        pass

    def __repr__(self):
        return self.CaseName + ' : ' + self.BodySite


class TreatmentNoteClass(BaseMethod):
    DateLastEdited: DateTimeClass
    Note: str
    StaffFirstName: str
    StaffLastName: str

    def __repr__(self):
        return self.DateLastEdited.__repr__() + ': ' + self.StaffFirstName + ' ' + self.StaffLastName


class QCLClass(BaseMethod):
    Description: str
    CreatedTime: DateTimeClass
    DueTime: DateTimeClass
    Completed: bool
    ResponsibleStaff: str
    CompletedStaff: str

    def __repr__(self):
        return (f"{self.Description} at {self.CreatedTime} due {self.DueTime}"
                f" was {self.Completed} responsible {self.ResponsibleStaff} and done by {self.CompletedStaff}")


class PatientClass(BaseMethod):
    RS_UID: str
    Patient_UID: int = 0
    Cases: List[CaseClass]
    DateLastModified: DateTimeClass
    MRN: str
    Name_First: str
    Name_Last: str
    Gender: int  # 0 M, 1 F, -1 Unknown
    DateOfBirth: DateTimeClass
    TreatmentNotes: List[TreatmentNoteClass]
    QCLs: List[QCLClass]

    def __init__(self):
        self.Name_First = ''
        self.Name_Last = ''
        self.Gender = -1
        self.Cases = []
        self.TreatmentNotes = []
        self.QCLs = []
        self.DateOfBirth = DateTimeClass()

    def define_rs_uid(self):
        illegal_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
        rs_id = self.MRN
        for c in illegal_chars:
            rs_id = rs_id.replace(c, "")
        self.RS_UID = rs_id

    def delete_unapproved_cases(self):
        """
        Delete cases without approved plans, and delete unapproved plans
        :return:
        """
        """
        First delete any unapproved plans
        """
        for case in self.Cases:
            case.delete_unapproved_plans()
        for case in self.Cases[:]:
            """
            If the case has no treatment plans left, means there are none that are approved, delete
            """
            if len(case.TreatmentPlans) == 0:
                self.Cases.remove(case)

    def save_header_to_directory(self, directory_path):
        patient_header = PatientHeader()
        patient_header.build(self)
        patient_header.save_to_directory(directory_path)

    def return_date_time_string_last_modified(self):
        last_mod = self.DateLastModified
        return f"{last_mod.year}.{last_mod.month}.{last_mod.day}.{last_mod.hour}.{last_mod.minute}"

    def return_out_file_name(self):
        out_file_name = f"{self.RS_UID}_{self.return_date_time_string_last_modified()}.json"
        return out_file_name

    def save_to_directory(self, directory_path):
        out_file_name = self.return_out_file_name()
        out_file = os.path.join(directory_path, out_file_name)
        self.to_json_file(out_file)
        self.save_header_to_directory(directory_path)
        if os.path.exists(out_file.replace(".json", ".txt")):
            os.remove(out_file.replace(".json", ".txt"))

    def __repr__(self):
        return self.MRN


class StrippedDownPlan(BaseMethod):
    PlanName: str
    PlannedBy: str or None
    Review: ReviewClass or None

    def build(self, treatment_plan: TreatmentPlanClass):
        self.PlanName = treatment_plan.PlanName
        if hasattr(treatment_plan, "PlannedBy"):
            self.PlannedBy = treatment_plan.PlannedBy
        if hasattr(treatment_plan, "Review"):
            self.Review = treatment_plan.Review

    def __repr__(self):
        return self.PlanName


class StrippedDownRegionOfInterest(BaseMethod):
    Name: str
    Type: str

    def __repr__(self):
        return self.Name


class StrippedDownCase(BaseMethod):
    CaseName: str
    BodySite: str
    ROIS: List[StrippedDownRegionOfInterest]
    POIS: List[str]
    TreatmentPlans: List[StrippedDownPlan]

    def __init__(self):
        self.ROIS = []
        self.POIS = []
        self.TreatmentPlans = []

    def delete_unapproved_plans(self):
        for tp in self.TreatmentPlans[:]:
            if tp.Review is None:
                self.TreatmentPlans.remove(tp)
            else:
                review: ReviewClass
                review = tp.Review
                if review.ApprovalStatus != "Approved":
                    self.TreatmentPlans.remove(tp)

    def build(self, case: CaseClass):
        self.CaseName = case.CaseName
        self.BodySite = case.BodySite
        for i in case.Base_ROIs:
            stripped_down_roi = StrippedDownRegionOfInterest()
            stripped_down_roi.Name = i.Name
            stripped_down_roi.Type = i.Type
            self.ROIS.append(stripped_down_roi)
        self.POIS = [i.Name for i in case.Base_POIs]
        for tp in case.TreatmentPlans:
            treatment_plan = StrippedDownPlan()
            treatment_plan.build(tp)
            self.TreatmentPlans.append(treatment_plan)

    def __repr__(self):
        return self.CaseName + ' : ' + self.BodySite


class PatientHeader(BaseMethod):
    MRN: str
    Name_First: str
    Name_Last: str
    Gender: int
    RS_UID: str
    FilePath: Union[str, bytes, os.PathLike]
    DateLastModified: DateTimeClass
    Cases: List[StrippedDownCase]
    TreatmentNotes: List[TreatmentNoteClass]
    QCLs: List[QCLClass]
    DateOfBirth: DateTimeClass

    def __init__(self):
        self.Cases = []
        self.TreatmentNotes = []
        self.QCLs = []
        self.Name_First = ''
        self.Name_Last = ''
        self.Gender = -1
        self.DateOfBirth = DateTimeClass()

    def define_rs_uid(self):
        illegal_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
        rs_id = self.MRN
        for c in illegal_chars:
            rs_id = rs_id.replace(c, "")
        self.RS_UID = rs_id

    def delete_unapproved_cases(self):
        """
        Delete cases without approved plans, and delete unapproved plans
        :return:
        """
        """
        First delete any unapproved plans
        """
        for case in self.Cases:
            case.delete_unapproved_plans()
        for case in self.Cases[:]:
            """
            If the case has no treatment plans left, means there are none that are approved, delete
            """
            if len(case.TreatmentPlans) == 0:
                self.Cases.remove(case)

    def save_to_directory(self, directory_path):
        last_mod = self.DateLastModified
        out_file_name = (f"{self.RS_UID}_"
                         f"{last_mod.year}.{last_mod.month}.{last_mod.day}.{last_mod.hour}.{last_mod.minute}")
        out_file_name += "_Header.json"
        out_file = os.path.join(directory_path, out_file_name)
        self.to_json_file(out_file)

    def build(self, patient: PatientClass):
        self.MRN = patient.MRN
        self.Gender = patient.Gender
        self.Name_Last = patient.Name_Last
        self.Name_First = patient.Name_First
        self.RS_UID = patient.RS_UID
        self.DateLastModified = patient.DateLastModified
        self.DateOfBirth = patient.DateOfBirth
        for case in patient.Cases:
            new_case = StrippedDownCase()
            new_case.build(case)
            self.Cases.append(new_case)
        for tx_note in patient.TreatmentNotes:
            new_note = TreatmentNoteClass()
            new_note.Note = tx_note.Note
            new_note.DateLastEdited = tx_note.DateLastEdited
            self.TreatmentNotes.append(new_note)
        for qcl in patient.QCLs:
            new_qcl = QCLClass()
            new_qcl.Description = qcl.Description
            new_qcl.CreatedTime = qcl.CreatedTime
            new_qcl.DueTime = qcl.DueTime
            new_qcl.Completed = qcl.Completed
            new_qcl.ResponsibleStaff = qcl.ResponsibleStaff
            new_qcl.CompletedStaff = qcl.CompletedStaff
            self.QCLs.append(new_qcl)

    def __repr__(self):
        return self.MRN


class PatientDatabase(BaseMethod):
    DBName: str
    Patients: Dict[str, PatientClass]
    Updated: bool

    def __init__(self, dbname):
        self.DBName = dbname
        self.Updated = False
        self.Patients = {}

    def delete_unapproved_patients(self):
        for key in list(self.Patients.keys()):
            patient = self.Patients[key]
            patient.delete_unapproved_cases()
            if len(patient.Cases) == 0:
                self.Patients.pop(key)

    def load_files(self, potential_files: List[str], tqdm=None):
        """

        :param potential_files: A list of full paths to a patient file
        :param tqdm:
        :return:
        """
        pbar = None
        print("Loading from " + self.DBName)
        if tqdm is not None:
            pbar = tqdm(total=len(potential_files), desc='Adding patients from ' + self.DBName)
        if load_parallel:
            thread_count = int(cpu_count() * 0.8 - 1)
            q = Queue(maxsize=thread_count)
            threads = []
            a = (q, pbar, self.Patients)
            for worker in range(thread_count):
                t = Thread(target=add_patient, args=(a,))
                t.start()
                threads.append(t)
            for file in potential_files:
                q.put(file)
            for _ in range(thread_count):
                q.put(None)
            for t in threads:
                t.join()
        else:
            for file in potential_files:
                try:
                    patient = PatientClass.from_json_file(file)
                    self.Patients[patient.RS_UID] = patient
                except:
                    continue
                if pbar is not None:
                    pbar.update()
        return None

    def load_from_directory(self, directory_path: Union[str, bytes, os.PathLike], specific_mrns: List[str] = None,
                            tqdm=None):
        patient: PatientClass
        all_files = [i for i in os.listdir(directory_path) if i.endswith(".json")]
        potential_files = [i for i in all_files if not i.endswith("_Header.json")]
        if specific_mrns:
            """
            If we have a list of specific MRNs to load, just load those patients
            """
            wanted_files = []
            for potential_file in potential_files:
                pat_mrn = "_".join(potential_file.split('_')[:-1])
                if pat_mrn in specific_mrns:
                    wanted_files.append(potential_file)
                elif pat_mrn.strip('0') in specific_mrns:
                    wanted_files.append(potential_file)
                elif max([pat_mrn.zfill(i) in specific_mrns for i in range(len(pat_mrn), max([len(pat_mrn) + 3, 10]))]):
                    wanted_files.append(potential_file)
            potential_files = wanted_files
        potential_files = [os.path.join(directory_path, i) for i in potential_files]
        self.load_files(potential_files=potential_files, tqdm=tqdm)

    def save_to_directory(self, directory_path: Union[str, bytes, os.PathLike]):
        for patient in self.Patients.values():
            patient.save_to_directory(directory_path)


class PatientHeaderDatabase(BaseMethod):
    DBName: str
    PatientHeaders: Dict[str, PatientHeader]

    def __init__(self, dbname):
        self.DBName = dbname
        self.PatientHeaders = {}

    def delete_unapproved_patients(self):
        for key in list(self.PatientHeaders.keys()):
            patient = self.PatientHeaders[key]
            patient.delete_unapproved_cases()
            if len(patient.Cases) == 0:
                self.PatientHeaders.pop(key)

    def load_files(self, potential_files: List[str], tqdm=None):
        pbar = None
        if tqdm is not None:
            pbar = tqdm(total=len(potential_files), desc='Adding patients from ' + self.DBName)
        if load_parallel:
            thread_count = int(cpu_count() * 0.8 - 1)
            q = Queue(maxsize=thread_count)
            threads = []
            a = (q, pbar, self.PatientHeaders)
            for worker in range(thread_count):
                t = Thread(target=add_patient_header, args=(a,))
                t.start()
                threads.append(t)
            for file in potential_files:
                q.put(file)
            for _ in range(thread_count):
                q.put(None)
            for t in threads:
                t.join()
        else:
            for file in potential_files:
                try:
                    patient_header = PatientHeader.from_json_file(file)
                    self.PatientHeaders[patient_header.RS_UID] = patient_header
                except:
                    continue
                if pbar is not None:
                    pbar.update()

    def load_from_directory(self, directory_path: Union[str, bytes, os.PathLike],
                            specific_mrns: List[str] = None, tqdm=None):
        patient: PatientHeader
        potential_files = [i for i in os.listdir(directory_path) if i.endswith("_Header.json")]
        if specific_mrns:
            specific_mrns = [str(i) for i in specific_mrns]
            """
            If we have a list of specific MRNs to load, just load those patients
            """
            wanted_files = []
            for potential_file in potential_files:
                pat_mrn = "_".join(potential_file.split('_')[:-2])
                if pat_mrn in specific_mrns:
                    wanted_files.append(potential_file)
                elif pat_mrn.strip('0') in specific_mrns or pat_mrn.lstrip('0') in specific_mrns:
                    wanted_files.append(potential_file)
                elif max([pat_mrn.zfill(i) in specific_mrns for i in range(len(pat_mrn), max([len(pat_mrn) + 3, 10]))]):
                    wanted_files.append(potential_file)
            potential_files = wanted_files
        print("Loading from " + self.DBName)
        potential_files = [os.path.join(directory_path, i) for i in potential_files]
        self.load_files(potential_files=potential_files, tqdm=tqdm)

    def return_patient_database(self, tqdm=None) -> PatientDatabase:
        """
        This is meant to return a full patient database from the header files present
        :return:
        """
        header_files = [i.FilePath for i in self.PatientHeaders.values()]
        pat_files = [i.replace("_Header.json", ".json") for i in header_files]
        potential_files = [i for i in pat_files if os.path.exists(i)]
        patient_database = PatientDatabase(self.DBName)
        patient_database.load_files(potential_files, tqdm=tqdm)
        return patient_database


class PatientDatabases(BaseMethod):
    Databases: Dict[str, PatientDatabase]

    def __init__(self):
        self.Databases = {}

    def add_database(self, patient_database: PatientDatabase):
        self.Databases[patient_database.DBName] = patient_database

    def delete_unapproved_patients(self):
        for db in self.Databases.values():
            db.delete_unapproved_patients()

    def save(self, database_path: Union[str, bytes, os.PathLike]):
        if not os.path.exists(database_path):
            os.makedirs(database_path)
        for db in self.Databases.values():
            print(f"Writing {db.DBName}")
            db_path = os.path.join(database_path, db.DBName)
            if not os.path.exists(db_path):
                os.makedirs(db_path)
            db.save_to_directory(db_path)

    def build_from_folder(self, path_to_database_directories: Union[str, bytes, os.PathLike],
                          specific_mrns: List[str] = None, tqdm=None):
        database_directories = []
        for root, database_directories, files in os.walk(path_to_database_directories):
            break
        for database_directory in database_directories:
            database = PatientDatabase(database_directory)
            database.load_from_directory(os.path.join(path_to_database_directories, database_directory),
                                         specific_mrns, tqdm)
            self.Databases[database_directory] = database


class PatientHeaderDatabases(BaseMethod):
    HeaderDatabases: Dict[str, PatientHeaderDatabase]

    def __init__(self):
        self.HeaderDatabases = {}

    def delete_unapproved_patients(self):
        for db in self.HeaderDatabases.values():
            db.delete_unapproved_patients()

    def return_patient_databases(self, tqdm=None) -> PatientDatabases:
        out_databases = PatientDatabases()
        for db in self.HeaderDatabases.values():
            out_databases.add_database(db.return_patient_database(tqdm))
        return out_databases

    def build_from_folder(self, path_to_database_directories: Union[str, bytes, os.PathLike],
                          specific_mrns: List[str] = None, tqdm=None):
        database_directories = []
        for root, database_directories, files in os.walk(path_to_database_directories):
            break
        for database_directory in database_directories:
            header_database = PatientHeaderDatabase(database_directory)
            header_database.load_from_directory(os.path.join(path_to_database_directories, database_directory),
                                                specific_mrns, tqdm)
            self.HeaderDatabases[database_directory] = header_database


def save_database(database: PatientDatabase, path: Union[str, bytes, os.PathLike]):
    for patient in database.Patients.values():
        patient.save_to_directory(path)


if __name__ == '__main__':
    pass
