import rdflib


def get_reachable_statements(node, graph, seen=None):
    """
    Computes fix point of all blank nodes reachable from passed node
    :return: list of statements
    """
    if seen is None:
        seen = {}
    if node in seen:
        return
    seen[node] = 1
    for stmt in graph.triples((node, None, None)):
        yield stmt
        obj = stmt[2]
        if isinstance(obj, rdflib.BNode):
            for i in get_reachable_statements(obj, graph, seen):
                yield i


def replace_uri_base(stmts, old_base, new_base):
    if old_base.endswith('/'):
        old_base = old_base[:-1]
    if new_base.endswith('/'):
        new_base = new_base[:-1]

    def replace_node(node):
        if isinstance(node, rdflib.URIRef):
            uri = node.toPython()
            if uri == old_base:
                node = rdflib.URIRef(new_base)
            elif uri.startswith(old_base) and uri[len(old_base)] == '/':
                node = rdflib.URIRef(new_base + uri[len(old_base):])
        return node

    for s, p, o in stmts:
        new_s = replace_node(s)
        new_p = replace_node(p)
        new_o = replace_node(o)
        yield (new_s, new_p, new_o)


# calc_hash_value is an implementation of an algorithm from
# Hashing of RDF graphs and a solution to the blank node problem
# Author(s): Hoefig, Edzard; Schieferdecker, Ina
# http://publica.fraunhofer.de/documents/N-326390.html


SUBJECT_START = u'{'
SUBJECT_END = u'}'
PROPERTY_START = u'('
PROPERTY_END = u')'
OBJECT_START = u'['
OBJECT_END = u']'
BLANK_NODE = u'*'


def calc_hash_value(g):
    subject_strings = []
    seen_subject = {}
    for ns in g.subjects():  # All subject nodes
        if ns in seen_subject:
            continue
        else:
            seen_subject[ns] = 1
        visited_nodes = {}
        subject_strings.append(encode_subject(ns, visited_nodes, g))
    subject_strings.sort()
    result = unicode()
    for s in subject_strings:
        result += SUBJECT_START + s + SUBJECT_END
    return result


def encode_subject(ns, visited_nodes, g):
    if isinstance(ns, rdflib.BNode):
        if ns in visited_nodes:
            return unicode()  # This path terminates
        else:
            visited_nodes[ns] = 1  # Record that we visited this node
            result = BLANK_NODE
    else:
        result = ns.toPython()  # ns has to be a IRI
    result += encode_properties(ns, visited_nodes, g)
    return result


def encode_properties(ns, visited_nodes, g):
    p = sorted(g.predicates(subject=ns), key=lambda x: x.toPython())
    result = unicode()
    #object_strings = []
    seen_iri = {}
    for iri in p:
        if iri in seen_iri:
            continue
        seen_iri[iri] = 1
        object_strings = []
        result += PROPERTY_START + iri.toPython()
        seen_no = {}
        for no in g.objects(subject=ns, predicate=iri):  # All objects for ns and iri
            if no in seen_no:
                continue
            seen_no[no] = 1
            object_strings.append(encode_object(no, visited_nodes, g))
        object_strings.sort()
        for o in object_strings:
            result += OBJECT_START + o + OBJECT_END
        result += PROPERTY_END
    return result


def encode_object(no, visited_nodes, g):
    if isinstance(no, rdflib.BNode):
        return encode_subject(no, visited_nodes, g)  # Re-enter Algorithm 2
    elif isinstance(no, rdflib.Literal):
        return no.normalize().n3()  # Consider language and type
    else:
        return no.toPython()  # no has to be a IRI
