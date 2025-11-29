# dataset_loader_classes.py

import pandas as pd
from datasets import load_dataset

class DatasetLoader:
    """
    Base class for dataset loaders.
    """
    def load(self):
        """
        Load the dataset.
        Must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method")


class HuggingFaceDatasetLoader(DatasetLoader):
    """
    Loads datasets from Hugging Face.
    """

    def __init__(self, dataset_name, split=None, **kwargs):
        """
        Parameters:
            dataset_name (str): Name of the Hugging Face dataset.
            split (str or None): Specific split to load ('train', 'test', 'validation').
                                 If None, loads all splits.
            **kwargs: Additional kwargs for load_dataset.
        """
        self.dataset_name = dataset_name
        self.split = split
        self.kwargs = kwargs

    def load(self):
        """
        Load the Hugging Face dataset.

        Returns:
            pd.DataFrame if split is specified.
            dict of pd.DataFrames if split is None (all splits).
        """
        if self.split:
            dataset = load_dataset(self.dataset_name, split=self.split, **self.kwargs)
            return pd.DataFrame(dataset)
        else:
            # Load all splits
            dataset_dict = load_dataset(self.dataset_name, **self.kwargs)
            return {split_name: pd.DataFrame(split_dataset)
                    for split_name, split_dataset in dataset_dict.items()}

