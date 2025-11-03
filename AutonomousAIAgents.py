import os
from openai import OpenAI

def subtasker_agent(task_prompt: str, client: OpenAI, num_subtasks: int = 3, log_dir: str = "Agent_Logs") -> list:
    """
    Generates structured subtasks for a task prompt, explicitly constrained to stay on-topic.

    Returns a list of dicts: [{'id': ..., 'instruction': ..., 'context': ...}]
    """
    system_message = (
        "You are a planning agent that breaks down writing assignments into tightly focused subtasks. "
        "Each subtask must be directly relevant to the original task and independently executable. "
        "Do not go beyond the scope of the topic. Avoid repeating content."
    )

    user_prompt = (
        f"The task is:\n\n{task_prompt}\n\n"
        f"Break this into exactly {num_subtasks} numbered subtasks. Each subtask should be one sentence. "
        "Only return a numbered list like:\n"
        "1. First subtask\n"
        "2. Second subtask\n"
        "...\n"
    )

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_prompt}
        ],
        temperature=1,
        max_tokens=1000
    )

    raw_output = response.choices[0].message.content.strip()

    # Parse and limit to num_subtasks
    subtasks = []
    for i, line in enumerate(raw_output.splitlines(), start=1):
        if line.strip():
            parts = line.split(".", 1)
            if len(parts) == 2:
                instruction = parts[1].strip()
                subtasks.append({
                    "id": f"task_{i}",
                    "instruction": instruction,
                    "context": task_prompt
                })
        if len(subtasks) >= num_subtasks:
            break

    return subtasks


def query_builder_agent(subtask_instruction: str, client: OpenAI, num_queries: int = 3, full_prompt: str = "") -> list:
    """
    Builds structured, focused retrieval queries based on a subtask and full prompt context.

    Returns a list of dicts: [{'query': ..., 'intent': ...}, ...]
    """
    system_msg = (
        "You are a research agent that builds precise, technically focused queries for content retrieval. "
        "Each query must be directly relevant to the subtask and consistent with the full task. "
        "Only return a numbered list of plain-text queries."
    )

    user_msg = (
        f"The full task is:\n\n{full_prompt}\n\n"
        f"The current subtask is:\n\n{subtask_instruction}\n\n"
        f"Generate exactly {num_queries} well-formed queries to guide document retrieval or AI research. "
        "Only output a numbered list of queries like:\n"
        "1. First query\n"
        "2. Second query\n"
        "...\n"
    )

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg}
        ],
        temperature=1,
        max_tokens=400
    )

    raw_output = response.choices[0].message.content.strip()

    # Parse and limit to requested count
    queries = []
    for i, line in enumerate(raw_output.splitlines(), start=1):
        parts = line.split(".", 1)
        if len(parts) == 2:
            query_text = parts[1].strip()
            if query_text:  # Skip empty lines
                queries.append({"query": query_text, "intent": "lookup"})
        if len(queries) >= num_queries:
            break

    return queries


def retrieval_agent(queries: list, client: OpenAI, model="gpt-3.5-turbo") -> list:
    """
    Executes GPT calls using each query string to retrieve relevant narrative content.

    Args:
        queries (list): List of {'query': ..., 'intent': ...}
        client (OpenAI): The OpenAI client
        model (str): Model to use

    Returns:
        List of {'query': ..., 'response': ...}
    """
    results = []

    for q in queries:
        try:
            print(f"[INFO] Retrieving for query: {q['query']}")
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": "You are a research assistant. Provide clear, accurate, and concise responses to queries."},
                    {"role": "user", "content": q["query"]}
                ],
                temperature=1,
                max_tokens=800
            )
            answer = response.choices[0].message.content.strip()
        except Exception as e:
            answer = f"[ERROR] Retrieval failed for '{q['query']}': {e}"

        results.append({
            "query": q["query"],
            "response": answer
        })

    return results

def drafting_agent(subtask: dict, client: OpenAI, model="gpt-4o") -> str:
    """
    Synthesizes a detailed, multiparagraph section draft from retrieved responses for a given subtask.

    Args:
        subtask (dict): A subtask object with 'instruction', 'queries', and 'retrievals'
        client (OpenAI): OpenAI API client
        model (str): Model to use (default: gpt-4o)

    Returns:
        str: A clean, consolidated draft paragraph
    """
    # Build context from all retrieved responses
    context_blocks = [r['response'] for r in subtask.get("retrievals", []) if r['response']]
    joined_context = "\n\n".join(context_blocks)

    # Compose messages
    system_msg = (
        "You are a professional NEPA technical writer. "
        "You will synthesize retrieved research into a clear, comprehensive, formal, and well-organized multi-paragraph write-up. "
        "You must include as much of the factual and technical content from the supporting information as possible, as long as it fits logically. "
        "Expand on points where appropriate to increase the depth and completeness of the discussion. "
        "Use full, complete paragraphs with logical transitions."
    )

    user_msg = (
        f"Subtask: {subtask['instruction']}\n\n"
        f"Supporting information:\n{joined_context}\n\n"
        "Write a detailed, comprehensive, multiparagraph draft that fully addresses the subtask using the provided supporting information. "
        "Incorporate as many details as possible while maintaining clarity and logical flow. Expand and elaborate where appropriate."
    )

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg}
            ],
            temperature=0.7,  # slightly lower for consistency
            max_tokens=2000   # allow much longer output
        )

        return response.choices[0].message.content.strip()

    except Exception as e:
        return f"[ERROR] Drafting failed for subtask '{subtask['instruction']}': {e}"


def consolidator_agent(prompt_text: str, subtasks: list, client: OpenAI, model="gpt-4o") -> str:
    """
    Consolidates multiple drafted subtask paragraphs into a single detailed, well-organized, multi-page NEPA section.

    Args:
        prompt_text: The original user prompt (e.g., "Write the Purpose and Need...")
        subtasks: List of dicts with 'instruction' and 'draft'
        client: OpenAI client

    Returns:
        A unified write-up that fully addresses the prompt with expanded, organized content.
    """
    combined_drafts = "\n\n".join(s['draft'] for s in subtasks)

    system_msg = (
        "You are a senior NEPA editor and technical writer tasked with combining multiple draft sections into one complete, professional write-up "
        "for a federal NEPA document (such as an Environmental Impact Statement or Environmental Assessment). "
        "The final version must:"
        "\n- Include as much technical detail as possible from the provided drafts"
        "\n- Eliminate redundancies"
        "\n- Improve transitions and flow logically from one concept to the next"
        "\n- Follow a formal, factual, and neutral tone"
        "\n- NOT include subtask numbers or headings"
        "\n- Be written as a cohesive, multi-page professional NEPA section."
    )

    user_msg = (
        f"Original Task Prompt:\n{prompt_text}\n\n"
        f"Drafted Subsections:\n{combined_drafts}\n\n"
        "Now write a fully consolidated version that satisfies the original task completely, maximizing technical content, flow, clarity, and completeness."
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_msg},
            {"role": "user", "content": user_msg}
        ],
        temperature=0.7,  # more stable and formal
        max_tokens=8000    # allow much longer output for multi-page sections
    )

    return response.choices[0].message.content.strip()



def write_final_section_to_doc(doc, prompt_title: str, prompt_text: str, final_writeup: str):
    """
    Cleanly writes the final consolidated  section into the Word document.

    Args:
        doc: The python-docx Document object
        prompt_title: The title of the prompt (e.g. "Project Description")
        prompt_text: The original user prompt
        final_writeup: The combined final draft
    """
    doc.add_heading(prompt_title, level=2)
    #doc.add_paragraph(prompt_text).italic = True
    doc.add_paragraph(final_writeup)
