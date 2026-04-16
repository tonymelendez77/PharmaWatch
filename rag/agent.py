import os
import sys
import json
from pathlib import Path

_ML_DIR = str(Path(__file__).resolve().parent.parent / "ml")
if _ML_DIR not in sys.path:
    sys.path.insert(0, _ML_DIR)

from langchain_groq import ChatGroq
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.tools import Tool
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

import retriever

GROQ_MODEL = "llama3-8b-8192"

SYSTEM_PROMPT = (
    "You are PharmaWatch, a drug safety intelligence assistant. "
    "You have access to FDA adverse event data, PubMed research, and Reddit patient sentiment. "
    "Always ground your answers in retrieved evidence. Never fabricate medical information. "
    "Always recommend consulting a healthcare provider."
)


def _llm():
    return ChatGroq(api_key=os.environ["GROQ_API_KEY"], model=GROQ_MODEL)


def _format_risk_summary(result):
    if result is None:
        return "No risk data available."
    lines = [
        "Serious reaction: {:.1f}%".format(float(result.get("serious_reaction_pct", 0.0))),
        "Hospitalization: {:.1f}%".format(float(result.get("hospitalization_pct", 0.0))),
        "Death: {:.1f}%".format(float(result.get("death_pct", 0.0))),
        "Disability: {:.1f}%".format(float(result.get("disability_pct", 0.0))),
        "Risk label: {}".format(result.get("risk_label", "UNKNOWN")),
        "Top risk factor: {}".format(result.get("top_risk_factor", "n/a")),
        "Dependency mention rate: {:.1f}%".format(float(result.get("dependency_mention_rate", 0.0))),
        "Withdrawal mention rate: {:.1f}%".format(float(result.get("withdrawal_mention_rate", 0.0))),
        "Community concern score: {:.1f}".format(float(result.get("community_concern_score", 0.0))),
        "Research risk consensus: {:.1f}%".format(float(result.get("research_risk_consensus", 0.0))),
        "Total research papers: {}".format(int(result.get("total_research_papers", 0))),
        "Label warning severity: {}".format(result.get("label_warning_severity", "UNKNOWN")),
    ]
    return "\n".join(lines)


def _format_research_results(results, drug_name):
    if not results:
        return "No relevant research papers found for {}.".format(drug_name)
    lines = []
    for r in results:
        snippet = (r.get("abstract") or "")[:300]
        lines.append(
            "- [{}] {} ({}): {}".format(
                r.get("article_id"),
                r.get("title"),
                r.get("publish_year"),
                snippet,
            )
        )
    return "\n".join(lines)


def build_agent(master_df=None, reddit_df=None, pubmed_df=None, labels_df=None):
    llm = _llm()

    def search_research(input_str):
        try:
            payload = json.loads(input_str)
        except (ValueError, TypeError):
            return "Invalid input. Expected JSON with keys 'query' and 'drug_name'."
        query = payload.get("query", "")
        drug_name = payload.get("drug_name", "")
        results = retriever.retrieve(query, drug_name)
        return _format_research_results(results, drug_name)

    def get_risk_summary(drug_name):
        if master_df is None:
            return "Risk data unavailable: master profile not loaded into agent."
        from predict import predict_risk
        result = predict_risk(drug_name, 30, 70, master_df, reddit_df, pubmed_df, labels_df)
        return _format_risk_summary(result)

    tools = [
        Tool(
            name="search_research",
            func=search_research,
            description=(
                "Search PubMed research papers about a specific drug. "
                "Input: JSON string with keys query and drug_name"
            ),
        ),
        Tool(
            name="get_risk_summary",
            func=get_risk_summary,
            description="Get risk scores and KPIs for a drug based on adverse event data",
        ),
    ]

    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        ("human", "{input}"),
        MessagesPlaceholder("agent_scratchpad"),
    ])

    agent = create_tool_calling_agent(llm, tools, prompt)
    return AgentExecutor(agent=agent, tools=tools, verbose=False)


def ask(question, drug_name, user_profile, predict_risk_result):
    executor = build_agent()

    risk_context = _format_risk_summary(predict_risk_result)
    human_input = (
        "Drug: {drug}\n"
        "User profile: {profile}\n\n"
        "Current risk assessment:\n{risk}\n\n"
        "Question: {question}"
    ).format(
        drug=drug_name,
        profile=user_profile,
        risk=risk_context,
        question=question,
    )

    response = executor.invoke({"input": human_input})
    return response.get("output", "")


def get_research_digest(drug_name, age_group, risk_label):
    papers = retriever.retrieve_for_profile(drug_name, age_group, risk_label, n_results=5)
    if not papers:
        return "No relevant research found for {}.".format(drug_name)

    papers_text = "\n\n".join(
        "[{}] {} ({}):\n{}".format(
            p.get("article_id"),
            p.get("title"),
            p.get("publish_year"),
            (p.get("abstract") or "")[:1000],
        )
        for p in papers
    )

    prompt = (
        "Summarize these research findings about {drug} for a {age} patient with {risk} risk. "
        "Focus on safety signals, key findings, and clinical relevance. Be concise.\n\n"
        "{papers}"
    ).format(
        drug=drug_name,
        age=age_group,
        risk=risk_label,
        papers=papers_text,
    )

    llm = _llm()
    response = llm.invoke(prompt)
    return getattr(response, "content", str(response))
