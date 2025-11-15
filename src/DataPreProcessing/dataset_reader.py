from datasets import Dataset
import pandas as pd

class DatasetReader:
    def __init__(self, dataset_or_path, file_type="csv", keep_extra_fields=True):
        self.keep_extra_fields = keep_extra_fields

        if isinstance(dataset_or_path, Dataset):
            # If a HF Dataset is passed, convert to pandas DataFrame
            self.df = pd.DataFrame(dataset_or_path)
        else:
            # Otherwise treat as file path
            self.file_type = file_type.lower()
            self.df = self._read_file(dataset_or_path)

        self.field_names = list(self.df.columns)
        self.input_fields = []
        self.output_fields = []
        self.extra_fields = []

    def _read_file(self, path):
        if self.file_type == "csv":
            return pd.read_csv(path)
        elif self.file_type in ["json", "jsonl"]:
            return pd.read_json(path, lines=True)
        elif self.file_type == "parquet":
            return pd.read_parquet(path)
        else:
            raise ValueError(f"Unsupported file type: {self.file_type}")

    def auto_detect_fields(self):
        """Auto-detect input/output fields and mark extras"""
        common_input = ["text", "input", "prompt", "user", "system", "messages"]

        # Detect input fields by name
        self.input_fields = [f for f in self.field_names if f.lower() in common_input]

        # Fallback: use first column if none matched
        if not self.input_fields and len(self.field_names) > 0:
            self.input_fields = [self.field_names[0]]

        if not self.output_fields and len(self.field_names) > 1:
            self.output_fields = [self.field_names[1]]

        # Extra fields to keep (optional)
        if self.keep_extra_fields:
            self.extra_fields = [f for f in self.field_names
                                 if f not in self.input_fields + self.output_fields]

        print("Detected input fields:", self.input_fields)
        print("Detected output fields:", self.output_fields)
        print("Extra fields kept:", self.extra_fields)

    def set_fields(self, input_fields=None, output_fields=None, extra_fields=None):
        if input_fields: self.input_fields = input_fields
        if output_fields: self.output_fields = output_fields
        if extra_fields: self.extra_fields = extra_fields

    def generate_dataset(self):
        """Produce Hugging Face Dataset with input/output and optional extras"""
        def make_row(row):
            input_text = " ".join([str(row[f]) for f in self.input_fields])
            output_text = " ".join([str(row[f]) for f in self.output_fields])
            extra = {f: row[f] for f in self.extra_fields} if self.extra_fields else {}
            record = {"input": input_text, "output": output_text}
            if self.keep_extra_fields and extra:
                record["extra_fields"] = extra
            return record

        records = [make_row(r) for r in self.df.to_dict(orient="records")]
        return Dataset.from_list(records)

    def to_cleaning_format(self):
        """
        Convert dataset to format compatible with cleaning pipeline.

        Returns dict records with 'text' field that cleaning steps expect.
        This bridges DatasetReader output with CleaningStep input requirements.

        Returns:
            List of dict records with 'text' field and metadata
        """
        records = []
        for row in self.df.to_dict(orient="records"):
            # Combine input fields into 'text' field for cleaning
            input_text = " ".join([str(row[f]) for f in self.input_fields if f in row])

            # Create record with 'text' field
            record = {"text": input_text}

            # Add output fields as metadata if they exist
            if self.output_fields:
                output_text = " ".join([str(row[f]) for f in self.output_fields if f in row])
                record["output"] = output_text

            # Add extra fields as metadata
            if self.keep_extra_fields and self.extra_fields:
                metadata = {f: row[f] for f in self.extra_fields if f in row}
                if metadata:
                    record["metadata"] = metadata

            records.append(record)

        return records
