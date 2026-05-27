from pydantic import BaseModel, Field
from typing import Optional, List, Literal


class InvoiceItem(BaseModel):
    name: str = Field(description="Name of the item")
    price: int = Field(description="Price of the item or total price for the quantity")


class InvoiceData(BaseModel):
    date: str = Field(description="Date of the invoice in YYYY-MM-DD format (Taiwan time)")
    amount: int = Field(description="Total amount of the invoice")
    vendor_tax_id: str = Field(default="", description="Tax ID of the vendor (8 digits). Empty string if not found.")
    buyer_tax_id: str = Field(default="", description="Tax ID of the buyer (8 digits). Empty string if not found.")
    items: List[InvoiceItem] = Field(description="List of items purchased")
    invoice_type: Literal["發票", "收據", "空白收據", "其他"] = Field(description="Type of the receipt/invoice")
    consumption_category: Literal["日常開銷與練習", "長期硬體設備購置", "社課開銷", "活動開銷", "未分類"] = Field(
        description="Predicted category of the consumption based on items"
    )


class ValidationResult(BaseModel):
    is_valid: bool
    eligibility: int
    missing_fields: List[str] = Field(default_factory=list)


class UserState(BaseModel):
    line_id: str
    user_name: str = "Unknown"
    state: Literal["NORMAL", "WAITING_FOR_INFO", "WAITING_FOR_CONFIRM"] = "NORMAL"
    temp_data: Optional[str] = None  # JSON string of current processing data
    last_used: Optional[str] = None

    def to_row(self) -> List[str]:
        return [
            self.line_id,
            self.user_name,
            self.state,
            self.temp_data or "",
            self.last_used or "",
        ]
