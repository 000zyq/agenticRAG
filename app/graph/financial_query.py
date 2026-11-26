"""Query typed financial observations without scanning report text."""
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP

from app.graph.financial import MAJOR_SUBSIDIARY_SCOPE, METRICS, SUBSIDIARY_MEMBERSHIP, SUBSIDIARY_SCOPE
from app.graph.query import _load_evidence_map
from app.storage.db import get_conn


UNIT_SCALE = {"元": 1, "千元": 1000, "万元": 10000, "亿元": 100000000}
PER_PERSON_NUMERATORS = ("revenue", "total_revenue", "parent_net_profit", "rd_investment")
PERSON_METRICS = ("employee_count", "rd_staff")
RANK_METRICS = {SUBSIDIARY_SCOPE: {"revenue", "net_profit"}, MAJOR_SUBSIDIARY_SCOPE: {"revenue", "parent_net_profit"}}


def rank_disclosure(rows, evidence, *, rank_by, sum_metric, top_k, scope=SUBSIDIARY_SCOPE):
    values = defaultdict(lambda: defaultdict(set))
    pages = defaultdict(set)
    for name, membership_id, fact_id, fact in rows:
        company = values[name]
        for relation_id in (membership_id, fact_id):
            pages[name].update(item["page"] for item in evidence.get(relation_id, []) if item["page"] is not None)
        if fact:
            company[fact["metric"]].add((fact["currency"], Decimal(fact["value"]) * UNIT_SCALE[fact["unit"]]))
    missing = sorted(name for name, facts in values.items() if len(facts[rank_by]) != 1)
    eligible = [(name, *next(iter(facts[rank_by]))) for name, facts in values.items() if len(facts[rank_by]) == 1]
    currencies = {currency for _, currency, _ in eligible}
    eligible.sort(key=lambda item: (-item[2], item[0]))
    selected = eligible[:top_k]
    evidence_pages = sorted({page for row in pages.values() for page in row})
    result = {"scope": scope, "full_group_coverage": False,
              "disclosed_company_count": len(values), "rank_metric": rank_by, "sum_metric": sum_metric,
              "companies_with_unique_rank_value": len(eligible),
              "requested_k": top_k, "missing_or_conflicting_rank_values": missing,
              "evidence_pages": evidence_pages, "ranked": [], "rank_total": None, "aggregate": None}
    reasons = []
    if not rows:
        reasons.append("no_disclosed_companies")
    if missing:
        reasons.append("missing_or_conflicting_rank_values")
    if len(currencies) > 1:
        reasons.append("mixed_currencies")
    if len(selected) < top_k:
        reasons.append("fewer_companies_with_values_than_requested")
    if reasons:
        return {**result, "status": "insufficient_comparable_disclosure", "reasons": reasons}
    if len(eligible) > top_k and selected[-1][2] == eligible[top_k][2]:
        return {**result, "status": "tie_at_cutoff"}
    ranked = []
    for name, currency, amount in selected:
        sum_values = values[name][sum_metric]
        sum_value = next(iter(sum_values))[1] if len(sum_values) == 1 and next(iter(sum_values))[0] == currency else None
        ranked.append({"company": name, "rank_value": str(amount),
                       "sum_value": str(sum_value) if sum_value is not None else None,
                       "evidence_pages": sorted(pages[name])})
    complete = all(row["sum_value"] is not None for row in ranked)
    return {**result, "status": "ok" if complete else "missing_or_conflicting_sum_values",
            "ranked": ranked, "currency": selected[0][1], "unit": "元",
            "rank_total": str(sum(amount for _, _, amount in selected)),
            "aggregate": str(sum(Decimal(row["sum_value"]) for row in ranked)) if complete else None}


class FinancialGraph:
    def __init__(self, doc_id):
        self.doc_id = doc_id

    def facts(self, metrics, year):
        if not isinstance(metrics, list) or not 1 <= len(metrics) <= len(METRICS):
            raise ValueError("Supply a nonempty list of metric codes")
        if not all(metric in METRICS.values() for metric in metrics) or type(year) is not int:
            raise ValueError("Unknown metric or invalid year")
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT r.relation_id, s.canonical_name, f.attributes
                FROM graph_relations r
                JOIN graph_entities s ON s.entity_id = r.subject_entity_id
                JOIN graph_entities f ON f.entity_id = r.object_entity_id
                WHERE r.doc_id = %s AND r.predicate_family = 'has_financial_fact'
                  AND NOT r.is_aggregated AND f.attributes->>'year' = %s
                  AND f.attributes->>'scope' = 'report_group'
                  AND f.attributes->>'metric' = ANY(%s)
                ORDER BY f.attributes->>'metric', r.relation_id
            """, (self.doc_id, str(year), metrics))
            rows = cur.fetchall()
        evidence = _load_evidence_map([row[0] for row in rows])
        result = [{"subject": subject, **fact, "evidence": evidence.get(relation_id, [])}
                  for relation_id, subject, fact in rows]
        return {"facts": result, "missing_metrics": sorted(set(metrics) - {row["metric"] for row in result})}

    def per_person(self, year, numerator_metric, people_metric, output_unit, decimal_places):
        if (numerator_metric not in PER_PERSON_NUMERATORS or people_metric not in PERSON_METRICS
                or output_unit not in UNIT_SCALE or type(decimal_places) is not int or not 0 <= decimal_places <= 6):
            raise ValueError("Choose a supported monetary metric, headcount, unit, and 0–6 decimal places")
        facts = self.facts([numerator_metric, people_metric], year)["facts"]
        numerator = [fact for fact in facts if fact["metric"] == numerator_metric]
        people = [fact for fact in facts if fact["metric"] == people_metric]
        result = {"year": year, "unit": f"{output_unit}/人", "value": None,
                  "numerator_observations": numerator, "headcount_observations": people}
        if not numerator or not people:
            return {**result, "status": "missing_inputs"}
        if (any(not fact["currency"] or fact["unit"] not in UNIT_SCALE for fact in numerator)
                or any(fact["unit"] != "人" for fact in people)):
            return {**result, "status": "incompatible_units"}
        amounts = {(fact["subject"], fact["currency"], Decimal(fact["value"]) * UNIT_SCALE[fact["unit"]])
                   for fact in numerator}
        counts = {(fact["subject"], Decimal(fact["value"])) for fact in people}
        if len(amounts) != 1 or len(counts) != 1:
            return {**result, "status": "conflicting_inputs"}
        subject, currency, amount = next(iter(amounts))
        people_subject, headcount = next(iter(counts))
        if subject != people_subject or headcount <= 0 or headcount != headcount.to_integral_value():
            return {**result, "status": "incompatible_observations"}
        scaled_amount = amount / UNIT_SCALE[output_unit]
        value = (scaled_amount / headcount).quantize(Decimal(1).scaleb(-decimal_places), rounding=ROUND_HALF_UP)
        return {**result, "status": "ok", "value": format(value, "f"), "currency": currency,
                "numerator_in_output_money_unit": format(scaled_amount, "f"), "headcount": format(headcount, "f"),
                "expression": f"{scaled_amount:f} {output_unit} / {headcount:f} 人"}

    def rank_subsidiaries(self, year, rank_by, sum_metric, top_k, scope=SUBSIDIARY_SCOPE):
        if type(year) is not int or type(top_k) is not int or not 1 <= top_k <= 20:
            raise ValueError("Supply an annual fiscal year and a top_k from 1 to 20")
        if scope not in RANK_METRICS:
            raise ValueError("Choose a supported subsidiary disclosure scope")
        if rank_by not in RANK_METRICS[scope] or sum_metric not in RANK_METRICS[scope]:
            raise ValueError(f"Supported ranking and sum metrics for {scope}: {sorted(RANK_METRICS[scope])}")
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                WITH facts AS (
                    SELECT financial.subject_entity_id, financial.relation_id, fact.attributes
                    FROM graph_relations financial
                    JOIN graph_entities fact ON fact.entity_id = financial.object_entity_id
                    WHERE financial.doc_id = %s AND financial.predicate_family = 'has_financial_fact'
                      AND NOT financial.is_aggregated AND fact.attributes->>'scope' = %s
                      AND fact.attributes->>'year' = %s AND fact.attributes->>'metric' = ANY(%s)
                )
                SELECT company.canonical_name, member.relation_id, facts.relation_id, facts.attributes
                FROM graph_relations member
                JOIN graph_entities company ON company.entity_id = member.object_entity_id
                LEFT JOIN facts ON facts.subject_entity_id = company.entity_id
                WHERE member.doc_id = %s AND member.predicate_family = %s
                  AND NOT member.is_aggregated
            """, (self.doc_id, scope, str(year), [rank_by, sum_metric], self.doc_id, SUBSIDIARY_MEMBERSHIP[scope]))
            rows = cur.fetchall()
        ids = sorted({relation for row in rows for relation in row[1:3] if relation is not None})
        return {"year": year, **rank_disclosure(rows, _load_evidence_map(ids), rank_by=rank_by,
                                                sum_metric=sum_metric, top_k=top_k, scope=scope)}

    def subsidiaries(self, location_terms, location_basis, *, company_names=None):
        if location_basis not in {"registered_in", "operates_in"}:
            raise ValueError("Choose registered_in or operates_in")
        if not isinstance(location_terms, list) or len(location_terms) > 30 or any(
            not isinstance(term, str) or not term.strip() or len(term) > 80 for term in location_terms
        ):
            raise ValueError("Supply up to 30 nonempty location terms")
        if company_names is not None and (
            not isinstance(company_names, list) or not 1 <= len(company_names) <= 20 or any(
                not isinstance(name, str) or not name.strip() or len(name) > 120 for name in company_names
            )
        ):
            raise ValueError("Supply 1 to 20 legal company names")
        company_filter = "AND company.canonical_name = ANY(%s)" if company_names else ""
        params = [location_basis, self.doc_id]
        if company_names:
            params.append(company_names)
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(f"""
                SELECT owned.object_entity_id, owned.relation_id, company.canonical_name,
                       loc.relation_id, place.canonical_name
                FROM graph_relations owned
                JOIN graph_entities company ON company.entity_id = owned.object_entity_id
                LEFT JOIN graph_relations loc ON loc.doc_id = owned.doc_id
                    AND loc.subject_entity_id = company.entity_id
                    AND loc.predicate_family = %s AND NOT loc.is_aggregated
                LEFT JOIN graph_entities place ON place.entity_id = loc.object_entity_id
                WHERE owned.doc_id = %s AND owned.predicate_family = 'has_reported_subsidiary'
                  AND NOT owned.is_aggregated
                  {company_filter}
                ORDER BY company.canonical_name, owned.relation_id, loc.relation_id
            """, params)
            rows = cur.fetchall()
        matching = {}
        terms = [term.strip().casefold() for term in location_terms]
        for entity_id, owned_id, company, loc_id, place in rows:
            if terms and (not place or not any(term in place.casefold() for term in terms)):
                continue
            hit = matching.setdefault(entity_id, {"company": company, "locations": set(), "relations": set()})
            hit["relations"].add(owned_id)
            if loc_id is not None:
                hit["locations"].add(place)
                hit["relations"].add(loc_id)
        preview = list(matching.values())[:20]
        evidence = _load_evidence_map(sorted({r for hit in matching.values() for r in hit["relations"]}))
        return {
            "scope": "subsidiaries extracted from disclosed subsidiary tables; complete group coverage unverified",
            "location_basis": location_basis, "location_terms": location_terms,
            "reported_subsidiaries_extracted": len({row[0] for row in rows}),
            "subsidiaries_with_location": len({row[0] for row in rows if row[3] is not None}),
            "match_count": len(matching), "truncated": len(matching) > len(preview),
            "evidence_pages": sorted({row["page"] for rows in evidence.values() for row in rows
                                      if row["page"] is not None}),
            "matches": [{"company": hit["company"], "locations": sorted(hit["locations"]),
                         "evidence_pages": sorted({row["page"] for r in hit["relations"]
                                                   for row in evidence.get(r, []) if row["page"] is not None})}
                        for hit in preview],
        }

    def company_locations(self, company_names, location_basis):
        return self.subsidiaries([], location_basis, company_names=company_names)
