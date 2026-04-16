from pydantic import BaseModel, Field


class PredictRequest(BaseModel):
    drug_name: str
    age: int = Field(ge=0, le=120)
    weight: float = Field(gt=0, le=500)


class AskRequest(BaseModel):
    drug_name: str
    age: int
    weight: float
    question: str


class DigestRequest(BaseModel):
    drug_name: str
    age_group: str
    risk_label: str


class ReactionItem(BaseModel):
    reaction: str
    frequency_pct: float


class ShapValue(BaseModel):
    feature: str
    contribution: float


class PredictResponse(BaseModel):
    drug_name: str
    serious_reaction_pct: float
    hospitalization_pct: float
    death_pct: float
    disability_pct: float
    risk_label: str
    dependency_mention_rate: float
    withdrawal_mention_rate: float
    community_concern_score: float
    research_risk_consensus: float
    total_research_papers: int
    top_5_reactions: list[ReactionItem]
    label_warning_severity: str
    known_interactions: list[str]
    shap_values: list[ShapValue]
    top_risk_factor: str


class AskResponse(BaseModel):
    answer: str


class DigestResponse(BaseModel):
    digest: str


class HealthResponse(BaseModel):
    status: str
    warehouse: str
