import os.path

try:
    from connect import *
except:
    x = 1
from .AbstractBase import *


class LoadPatientClass(object):
    def __init__(self, patient_db):
        self.patient_db = patient_db

    def load_patient_from_info(self, info):
        patient = self.patient_db.LoadPatient(PatientInfo=info, AllowPatientUpgrade=False)
        return patient

    def load_patient(self, mrn: str):
        info = self.patient_db.QueryPatientInfo(Filter={"PatientID": mrn}, UseIndexService=False)
        patient = None
        if info:
            patient = self.patient_db.LoadPatient(PatientInfo=info[0], AllowPatientUpgrade=False)
        return patient


def fix_string_for_folder(string: str):
    illegal_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
    for c in illegal_chars:
        string = string.replace(c, "")
    return string


class ExportBaseClass(object):
    PatientLoader: LoadPatientClass
    Patient: PatientClass
    BaseExportPath: Union[str, bytes, os.PathLike]
    RSPatient: None

    def __init__(self):
        self.PatientLoader = LoadPatientClass(get_current("PatientDB"))
        self.RSPatient = None

    def set_export_path(self, path: Union[str, bytes, os.PathLike]):
        self.BaseExportPath = path

    def set_patient(self, patient: PatientClass):
        self.RSPatient = self.PatientLoader.load_patient(patient.MRN)

    def export_all_in_patient(self, patient: PatientClass):
        self.set_patient(patient)
        self.export_examinations_and_structures(patient)
        self.export_dose(patient)

    def export_current_examination(self, rs_case, rs_exam):
        export_path = os.path.join(self.BaseExportPath, rs_exam.Name)
        if not os.path.exists(export_path):
            os.makedirs(export_path)
        try:
            rs_case.ScriptableDicomExport(ExportFolderPath=export_path,
                                          Examinations=[rs_exam.Name])
        except:
            rs_case.ScriptableDicomExport(ExportFolderPath=export_path,
                                          Examinations=[rs_exam.Name],
                                          IgnorePreConditionWarnings=True)

    def export_examinations(self, patient: PatientClass):
        for case in patient.Cases:
            case_names = [c.CaseName for c in self.RSPatient.Cases]
            if case.CaseName not in case_names:
                continue
            rs_case = self.RSPatient.Cases[case.CaseName]
            case_name = fix_string_for_folder(case.CaseName)
            rs_exams = [e.Name for e in rs_case.Examinations]
            for exam in case.Examinations:
                if exam.ExamName not in rs_exams:
                    continue
                exam_name = fix_string_for_folder(exam.ExamName)
                export_path = os.path.join(self.BaseExportPath, patient.RS_UID, case_name, exam_name)
                if not os.path.exists(export_path):
                    os.makedirs(export_path)
                try:
                    rs_case.ScriptableDicomExport(ExportFolderPath=export_path,
                                                  Examinations=[exam.ExamName])
                except:
                    rs_case.ScriptableDicomExport(ExportFolderPath=export_path,
                                                  Examinations=[exam.ExamName],
                                                  IgnorePreConditionWarnings=True)

    def export_rois_as_meta_images(self, patient: PatientClass):
        for case in patient.Cases:
            case_names = [c.CaseName for c in self.RSPatient.Cases]
            if case.CaseName not in case_names:
                continue
            rs_case = self.RSPatient.Cases[case.CaseName]
            case_name = fix_string_for_folder(case.CaseName)
            rs_exams = [e.Name for e in rs_case.Examinations]
            for exam in case.Examinations:
                if exam.ExamName not in rs_exams:
                    continue
                exam_name = fix_string_for_folder(exam.ExamName)
                rs_structure_set = rs_case.PatientModel.StructureSets[exam.ExamName].RoiGeometries
                rs_roi_names = [r.OfRoi.Name for r in rs_structure_set]
                for roi in exam.ROIs:
                    if roi.Name not in rs_roi_names:
                        continue
                    rs_roi = rs_structure_set[roi.Name]
                    if not rs_roi.HasContours():
                        continue
                    export_path = os.path.join(self.BaseExportPath, patient.RS_UID, case_name, exam_name)
                    if not os.path.exists(export_path):
                        os.makedirs(export_path)
                    meta_file_name = os.path.join(export_path, "{}.mhd".format(roi.Name))
                    if not os.path.exists(meta_file_name):
                        rs_roi.ExportRoiGeometryAsMetaImage(MetaFileName=meta_file_name, AsExamination=True)

    def export_examinations_and_structures(self, patient: PatientClass):
        for case in patient.Cases:
            case_names = [c.CaseName for c in self.RSPatient.Cases]
            if case.CaseName not in case_names:
                continue
            rs_case = self.RSPatient.Cases[case.CaseName]
            case_name = fix_string_for_folder(case.CaseName)
            rs_exams = [e.Name for e in rs_case.Examinations]
            for exam in case.Examinations:
                if exam.ExamName not in rs_exams:
                    continue
                exam_name = fix_string_for_folder(exam.ExamName)
                export_path = os.path.join(self.BaseExportPath, patient.RS_UID, case_name, exam_name)
                if not os.path.exists(export_path):
                    os.makedirs(export_path)
                try:
                    rs_case.ScriptableDicomExport(ExportFolderPath=export_path,
                                                  Examinations=[exam.ExamName],
                                                  RtStructureSetsForExaminations=[exam.ExamName])
                except:
                    rs_case.ScriptableDicomExport(ExportFolderPath=export_path,
                                                  Examinations=[exam.ExamName],
                                                  RtStructureSetsForExaminations=[exam.ExamName],
                                                  IgnorePreConditionWarnings=True)

    def export_dose(self, patient: PatientClass):
        for case in patient.Cases:
            case_names = [c.CaseName for c in self.RSPatient.Cases]
            if case.CaseName not in case_names:
                continue
            rs_case = self.RSPatient.Cases[case.CaseName]
            for treatment_plan in case.TreatmentPlans:
                plan_name = treatment_plan.PlanName
                for beam_set in treatment_plan.BeamSets:
                    beam_set_name = beam_set.DicomPlanLabel
                    export_path = os.path.join(self.BaseExportPath, patient.RS_UID, "Case_{}".format(case.Case_UID),
                                               "Plan_{}".format(treatment_plan.TreatmentPlan_UID),
                                               "Beam_{}".format(beam_set.BeamSetUID))
                    if not os.path.exists(export_path):
                        os.makedirs(export_path)
                    rs_case.ScriptableDicomExport(ExportFolderPath=export_path,
                                                  BeamSetDoseForBeamSets=["%s:%s" % (plan_name, beam_set_name)])


if __name__ == '__main__':
    pass