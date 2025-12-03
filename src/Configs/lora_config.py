from dataclasses import dataclass, field
from typing import List, Optional, Union
import torch

@dataclass
class LoraConfig:
    """
    Configuration for Low-Rank Adaptation (LoRA) fine-tuning.
    
    Attributes:
        r (int): Lora attention dimension (rank).
        lora_alpha (int): The alpha parameter for Lora scaling.
        target_modules (List[str] or str): The names of the modules to apply Lora to.
        lora_dropout (float): The dropout probability for Lora layers.
        bias (str): Bias type for Lora. Can be 'none', 'all' or 'lora_only'.
        task_type (str): The task type for the model (e.g., 'CAUSAL_LM', 'SEQ_2_SEQ_LM').
        modules_to_save (List[str]): List of modules apart from LoRA layers to be set as trainable and saved in the final checkpoint.
    """
    r: int = 8
    lora_alpha: int = 32
    target_modules: Union[List[str], str] = field(default_factory=lambda: ["q_proj", "v_proj"])
    lora_dropout: float = 0.05
    bias: str = "none"
    task_type: str = "CAUSAL_LM"
    modules_to_save: Optional[List[str]] = None
    quantization: str = "4-bit"

    def to_dict(self):
        """Returns the configuration as a dictionary."""
        return {
            "r": self.r,
            "lora_alpha": self.lora_alpha,
            "target_modules": self.target_modules,
            "lora_dropout": self.lora_dropout,
            "bias": self.bias,
            "task_type": self.task_type,
            "modules_to_save": self.modules_to_save
        }

        
@dataclass 
class TrainingParams:
    epochs: int
    batch_size: int
    training_data_path: str
    train_test_split: float
    grad_accumulation_steps: int 
    learning_rate: float
    weight_decay: float
    max_grad_norm: float
    save_every: int
    optimizer: str = "adam"
    data_type: str = "bfloat16"
    device_map: str = "cuda" if torch.cuda.is_available() else "cpu"


class TrainingConfig:
    model_name: str
    lora_config: LoraConfig
    training_params: TrainingParams
    dataset_path: str #this can be changed depending on how we're storing the data 
    


    



    
    