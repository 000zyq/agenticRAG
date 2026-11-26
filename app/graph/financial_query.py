"""Query typed financial observations without scanning report text."""
from app.graph.financial import METRICS
from app.graph.query import _load_evidence_map
from app.storage.db import get_conn


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
                  AND f.attributes->>'metric' = ANY(%s)
                ORDER BY f.attributes->>'metric', r.relation_id
            """, (self.doc_id, str(year), metrics))
            rows = cur.fetchall()
        evidence = _load_evidence_map([row[0] for row in rows])
        result = [{"subject": subject, **fact, "evidence": evidence.get(relation_id, [])}
                  for relation_id, subject, fact in rows]
        return {"facts": result, "missing_metrics": sorted(set(metrics) - {row["metric"] for row in result})}

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
