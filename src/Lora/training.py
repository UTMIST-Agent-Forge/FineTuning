from transformers import (
    AutoModelForCausalLM, 
    AutoTokenizer,
    BitsAndBytesConfig,
    TrainingArguments,
    get_linear_schedule_with_warmup,
    PreTrainedTokenizer,
    PreTrainedModel
)
from peft import get_peft_model, LoraConfig as PeftLoraConfig, prepare_model_for_kbit_training, PeftModel
from torch.utils.data import DataLoader, Dataset, random_split
from torch.optim import AdamW, Optimizer
from torch.optim.lr_scheduler import LRScheduler
import torch
from pathlib import Path
from typing import Optional, Dict, Any, Union, Literal, cast
import logging
from tqdm import tqdm
import json

from Configs.lora_config import LoraConfig, TrainingConfig, TrainingParams
from DataPreProcessing.dataset_reader import DatasetReader

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class LoRATrainer:
    """
    Trainer class for fine-tuning language models using LoRA.
    """
    
    def __init__(
        self,
        model_name: str,
        lora_config: LoraConfig,
        training_params: TrainingParams,
        output_dir: str = "./checkpoints"
    ):
        """
        Initialize the LoRA trainer.
        
        Args:
            model_name: Name or path of the pretrained model
            lora_config: LoRA configuration
            training_params: Training parameters
            output_dir: Directory to save checkpoints and logs
        """
        self.model_name = model_name
        self.lora_config = lora_config
        self.training_params = training_params
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.device = torch.device(training_params.device_map)
        self.model: Any = None
        self.tokenizer: Optional[PreTrainedTokenizer] = None
        self.optimizer: Optional[Optimizer] = None
        self.scheduler: Optional[LRScheduler] = None
        
        logger.info(f"Initialized LoRATrainer for model: {model_name}")
        logger.info(f"Device: {self.device}")
        
    def _get_quantization_config(self) -> Optional[BitsAndBytesConfig]:
        """
        Get quantization configuration based on lora_config.
        
        Returns:
            BitsAndBytesConfig or None
        """
        if self.lora_config.quantization == "4-bit":
            return BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_use_double_quant=True,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_compute_dtype=torch.bfloat16 if self.training_params.data_type == "bfloat16" else torch.float16
            )
        elif self.lora_config.quantization == "8-bit":
            return BitsAndBytesConfig(
                load_in_8bit=True
            )
        return None
    
    def load_model(self):
        """
        Load and prepare the model with LoRA adapters.
        """
        logger.info(f"Loading model: {self.model_name}")
        
        # Get quantization config
        quantization_config = self._get_quantization_config()
        
        # Load tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        if self.tokenizer.pad_token is None:  # type: ignore
            self.tokenizer.pad_token = self.tokenizer.eos_token  # type: ignore
        
        # Load base model
        self.model = AutoModelForCausalLM.from_pretrained(
            self.model_name,
            quantization_config=quantization_config,
            device_map="auto" if quantization_config else self.device,
            torch_dtype=torch.bfloat16 if self.training_params.data_type == "bfloat16" else torch.float16,
            trust_remote_code=True
        )
        
        # Prepare model for k-bit training if using quantization
        if quantization_config:
            self.model = prepare_model_for_kbit_training(self.model)
        
        # Configure LoRA
        bias_value = cast(Literal["none", "all", "lora_only"], self.lora_config.bias)
        peft_config = PeftLoraConfig(
            r=self.lora_config.r,
            lora_alpha=self.lora_config.lora_alpha,
            target_modules=self.lora_config.target_modules,
            lora_dropout=self.lora_config.lora_dropout,
            bias=bias_value,
            task_type=self.lora_config.task_type,
            modules_to_save=self.lora_config.modules_to_save
        )
        
        # Apply LoRA
        self.model = get_peft_model(self.model, peft_config)  # type: ignore
        self.model.print_trainable_parameters()  # type: ignore
        
        logger.info("Model loaded and LoRA adapters applied successfully")
        
    def prepare_optimizer_and_scheduler(self, num_training_steps: int):
        """
        Prepare optimizer and learning rate scheduler.
        
        Args:
            num_training_steps: Total number of training steps
        """
        if self.model is None:
            raise ValueError("Model must be loaded before preparing optimizer")
        
        # Get trainable parameters
        trainable_params = [p for p in self.model.parameters() if p.requires_grad]
        
        # Initialize optimizer
        if self.training_params.optimizer.lower() == "adam":
            self.optimizer = AdamW(
                trainable_params,
                lr=self.training_params.learning_rate,
                weight_decay=self.training_params.weight_decay
            )
        else:
            raise ValueError(f"Unsupported optimizer: {self.training_params.optimizer}")
        
        # Initialize scheduler
        num_warmup_steps = int(0.1 * num_training_steps)  # 10% warmup
        self.scheduler = get_linear_schedule_with_warmup(
            self.optimizer,
            num_warmup_steps=num_warmup_steps,
            num_training_steps=num_training_steps
        )
        
        logger.info(f"Optimizer: {self.training_params.optimizer}")
        logger.info(f"Learning rate: {self.training_params.learning_rate}")
        logger.info(f"Warmup steps: {num_warmup_steps}/{num_training_steps}")
    
    def train_epoch(
        self, 
        train_loader: DataLoader, 
        epoch: int
    ) -> Dict[str, float]:
        """
        Train for one epoch.
        
        Args:
            train_loader: Training data loader
            epoch: Current epoch number
            
        Returns:
            Dictionary with training metrics
        """
        if self.model is None or self.optimizer is None or self.scheduler is None:
            raise ValueError("Model, optimizer, and scheduler must be initialized before training")
        
        self.model.train()
        total_loss = 0
        num_batches = len(train_loader)
        
        progress_bar = tqdm(train_loader, desc=f"Epoch {epoch}/{self.training_params.epochs}")
        
        for batch_idx, batch in enumerate(progress_bar):
            # Move batch to device
            input_ids = batch["input_ids"].to(self.device)
            attention_mask = batch["attention_mask"].to(self.device)
            labels = batch["labels"].to(self.device) if "labels" in batch else input_ids
            
            # Forward pass
            outputs = self.model(
                input_ids=input_ids,
                attention_mask=attention_mask,
                labels=labels
            )
            
            loss = outputs.loss / self.training_params.grad_accumulation_steps
            
            # Backward pass
            loss.backward()
            
            # Update weights after gradient accumulation
            if (batch_idx + 1) % self.training_params.grad_accumulation_steps == 0:
                # Gradient clipping
                torch.nn.utils.clip_grad_norm_(
                    self.model.parameters(),
                    self.training_params.max_grad_norm
                )
                
                # Optimizer step
                self.optimizer.step()
                self.scheduler.step()
                self.optimizer.zero_grad()
            
            total_loss += loss.item() * self.training_params.grad_accumulation_steps
            
            # Update progress bar
            progress_bar.set_postfix({
                'loss': f'{loss.item() * self.training_params.grad_accumulation_steps:.4f}',
                'lr': f'{self.scheduler.get_last_lr()[0]:.2e}'
            })
        
        avg_loss = total_loss / num_batches
        return {"loss": avg_loss}
    
    def validate(self, val_loader: DataLoader) -> Dict[str, float]:
        """
        Validate the model.
        
        Args:
            val_loader: Validation data loader
            
        Returns:
            Dictionary with validation metrics
        """
        if self.model is None:
            raise ValueError("Model must be loaded before validation")
        
        self.model.eval()
        total_loss = 0
        num_batches = len(val_loader)
        
        with torch.no_grad():
            for batch in tqdm(val_loader, desc="Validation"):
                input_ids = batch["input_ids"].to(self.device)
                attention_mask = batch["attention_mask"].to(self.device)
                labels = batch["labels"].to(self.device) if "labels" in batch else input_ids
                
                outputs = self.model(
                    input_ids=input_ids,
                    attention_mask=attention_mask,
                    labels=labels
                )
                
                total_loss += outputs.loss.item()
        
        avg_loss = total_loss / num_batches
        return {"val_loss": avg_loss}
    
    def save_checkpoint(self, epoch: int, metrics: Dict[str, float]):
        """
        Save model checkpoint.
        
        Args:
            epoch: Current epoch
            metrics: Training metrics to save
        """
        if self.model is None or self.tokenizer is None:
            raise ValueError("Model and tokenizer must be loaded before saving")
        if self.optimizer is None or self.scheduler is None:
            raise ValueError("Optimizer and scheduler must be initialized before saving")
        
        checkpoint_dir = self.output_dir / f"checkpoint-epoch-{epoch}"
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        # Save LoRA adapters
        self.model.save_pretrained(str(checkpoint_dir))
        self.tokenizer.save_pretrained(str(checkpoint_dir))
        
        # Save training state
        state = {
            "epoch": epoch,
            "metrics": metrics,
            "optimizer_state_dict": self.optimizer.state_dict(),
            "scheduler_state_dict": self.scheduler.state_dict(),
        }
        torch.save(state, checkpoint_dir / "training_state.pt")
        
        # Save config
        config = {
            "model_name": self.model_name,
            "lora_config": self.lora_config.to_dict(),
            "training_params": vars(self.training_params)
        }
        with open(checkpoint_dir / "config.json", "w") as f:
            json.dump(config, f, indent=2)
        
        logger.info(f"Checkpoint saved to {checkpoint_dir}")
    
    def train(
        self,
        train_dataset: Dataset,
        val_dataset: Optional[Dataset] = None
    ):
        """
        Main training loop.
        
        Args:
            train_dataset: Training dataset
            val_dataset: Validation dataset (optional)
        """
        # Create data loaders
        train_loader = DataLoader(
            train_dataset,
            batch_size=self.training_params.batch_size,
            shuffle=True,
            num_workers=0  # Adjust based on your system
        )
        
        val_loader = None
        if val_dataset is not None:
            val_loader = DataLoader(
                val_dataset,
                batch_size=self.training_params.batch_size,
                shuffle=False,
                num_workers=0
            )
        
        # Calculate total training steps
        num_training_steps = (
            len(train_loader) * self.training_params.epochs
        ) // self.training_params.grad_accumulation_steps
        
        # Prepare optimizer and scheduler
        self.prepare_optimizer_and_scheduler(num_training_steps)
        
        logger.info("=" * 50)
        logger.info("Starting training")
        logger.info(f"Epochs: {self.training_params.epochs}")
        logger.info(f"Batch size: {self.training_params.batch_size}")
        logger.info(f"Gradient accumulation steps: {self.training_params.grad_accumulation_steps}")
        logger.info(f"Total training steps: {num_training_steps}")
        logger.info("=" * 50)
        
        # Training loop
        for epoch in range(1, self.training_params.epochs + 1):
            logger.info(f"\nEpoch {epoch}/{self.training_params.epochs}")
            
            # Train
            train_metrics = self.train_epoch(train_loader, epoch)
            logger.info(f"Train Loss: {train_metrics['loss']:.4f}")
            
            # Validate
            if val_loader is not None:
                val_metrics = self.validate(val_loader)
                logger.info(f"Val Loss: {val_metrics['val_loss']:.4f}")
                train_metrics.update(val_metrics)
            
            # Save checkpoint
            if epoch % self.training_params.save_every == 0:
                self.save_checkpoint(epoch, train_metrics)
        
        # Save final model
        logger.info("\nTraining completed!")
        self.save_checkpoint(self.training_params.epochs, train_metrics)


class TextDataset(Dataset):
    """
    Dataset for text data.
    """
    def __init__(self, texts: list[str], tokenizer: PreTrainedTokenizer, max_length: int = 512):
        self.texts = texts
        self.tokenizer = tokenizer
        self.max_length = max_length

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        text = str(self.texts[idx])
        
        # Tokenize
        encoding = self.tokenizer(
            text,
            truncation=True,
            max_length=self.max_length,
            padding="max_length",
            return_tensors="pt"
        )
        
        # Remove batch dimension added by tokenizer
        input_ids = encoding["input_ids"]
        attention_mask = encoding["attention_mask"]
        
        # Ensure tensors and squeeze batch dimension
        if torch.is_tensor(input_ids):
            input_ids = input_ids.squeeze(0)
        if torch.is_tensor(attention_mask):
            attention_mask = attention_mask.squeeze(0)
        
        return {
            "input_ids": input_ids,
            "attention_mask": attention_mask,
            "labels": input_ids.clone() if torch.is_tensor(input_ids) else input_ids  # For causal LM
        }


def create_dataset_from_path(
    data_path: str,
    tokenizer: PreTrainedTokenizer,
    max_length: int = 512
) -> TextDataset:
    """
    Create a dataset from a file path.
    
    Args:
        data_path: Path to the training data
        tokenizer: Tokenizer to use
        max_length: Maximum sequence length
        
    Returns:
        PyTorch Dataset
    """
    # Determine file type from extension
    data_path_str = str(data_path)
    file_type = "csv"
    if data_path_str.endswith(".json") or data_path_str.endswith(".jsonl"):
        file_type = "json"
    elif data_path_str.endswith(".parquet"):
        file_type = "parquet"
        
    # Load data using DatasetReader
    try:
        reader = DatasetReader(data_path, file_type=file_type)
        reader.auto_detect_fields()
        
        if not reader.input_fields:
            logger.warning("No input fields detected automatically. Using the first column.")
            if not reader.df.columns.empty:
                text_column = reader.df.columns[0]
            else:
                raise ValueError("Dataset is empty or has no columns")
        else:
            text_column = reader.input_fields[0]
            
        logger.info(f"Using column '{text_column}' as input text")
        texts = reader.df[text_column].tolist()
        
        return TextDataset(texts, tokenizer, max_length)
        
    except Exception as e:
        logger.error(f"Error creating dataset: {e}")
        raise e


def main():
    """
    Example usage of the LoRATrainer.
    """
    # Define configuration
    lora_config = LoraConfig(
        r=16,
        lora_alpha=32,
        target_modules=["q_proj", "v_proj", "k_proj", "o_proj"],
        lora_dropout=0.05,
        bias="none",
        task_type="CAUSAL_LM",
        quantization="4-bit"
    )
    
    training_params = TrainingParams(
        epochs=3,
        batch_size=4,
        training_data_path="./data/train.json",
        train_test_split=0.9,
        grad_accumulation_steps=4,
        learning_rate=2e-4,
        weight_decay=0.01,
        max_grad_norm=1.0,
        save_every=1,
        optimizer="adam",
        data_type="bfloat16"
    )
    
    # Initialize trainer
    trainer = LoRATrainer(
        model_name="meta-llama/Llama-2-7b-hf",  # Replace with your model
        lora_config=lora_config,
        training_params=training_params,
        output_dir="./checkpoints"
    )
    
    # Load model
    trainer.load_model()
    
    if trainer.tokenizer is None:
        raise ValueError("Tokenizer not initialized")

    # Load and prepare datasets
    try:
        full_dataset = create_dataset_from_path(
            training_params.training_data_path,
            trainer.tokenizer
        )
        
        # Split into train and validation
        train_size = int(training_params.train_test_split * len(full_dataset))
        val_size = len(full_dataset) - train_size
        train_dataset, val_dataset = random_split(full_dataset, [train_size, val_size])
        
        logger.info(f"Dataset split: {len(train_dataset)} training, {len(val_dataset)} validation")
        
        # Start training
        trainer.train(train_dataset, val_dataset)
        
    except Exception as e:
        logger.error(f"Failed to start training: {e}")

if __name__ == "__main__":
    main()
