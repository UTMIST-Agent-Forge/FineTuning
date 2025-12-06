from cleaningStep import CleaningStep
from dataset_reader import DatasetReader
from dataset_loader import HuggingFaceDatasetLoader
from dataLoading import Data

# cleaning steps
from standardizer import Standardizer
from qualityFilter import QualityFilter
from removeDuplicates import RemoveDuplicates
from extractMetaData import Metadata

class DataPreProcessingController():

    def __init__(self, document_id: str):        
        data = Data(document_id)
        self.records = data.records
        self.cleaned_records = None

    def runPreProcessingSteps(self):
        cleaning_steps = [
            Standardizer(),
            QualityFilter(min_length=5, max_length=100),
            RemoveDuplicates(selected_key='text'),
            Metadata()
        ]

        self.cleaned_records = self.records.copy()
        for step in cleaning_steps:
            step_name = step.__class__.__name__
            before = len(self.cleaned_records)

            temp = []
            for record in self.cleaned_records:
                result = step.process(record)
                if result is not None:
                    temp.append(result if isinstance(result, dict) else result.to_dict())

            self.cleaned_records = temp
            after = len(self.cleaned_records)
            print(f"   {step_name}: {before} -> {after} records")

