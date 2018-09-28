## begin license ##
#
# "Meresco SequentialStore" contains components facilitating efficient sequentially ordered storing and retrieval.
#
# Copyright (C) 2018 Seecr (Seek You Too B.V.) http://seecr.nl
#
# This file is part of "Meresco SequentialStore"
#
# "Meresco SequentialStore" is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# "Meresco SequentialStore" is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with "Meresco SequentialStore"; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
#
## end license ##

import sys
from os import makedirs, listdir
from os.path import isdir, join, getsize, isfile
from shutil import rmtree
from random import randint
from time import time, sleep

from seecr.test import SeecrTestCase
from seecr.test.utils import sleepWheel

from meresco.sequentialstore import SequentialStorage


class DiskSpaceTest(SeecrTestCase):
    def testBigAndRandomlyOverwrittenStore(self):
        def makeId(i):
            return "http://example.org/identifier/%s" % i

        N = 100000
        M = N * 4
        directory = '/data/test/diskspacetest'
        if isdir(directory):
            rmtree(directory)
        storeDir = join(directory, 'store')
        makedirs(storeDir)

        c = SequentialStorage(storeDir)

        print 'size???', getSimpleDirSize(storeDir)
        sys.stdout.flush()

        with open(join(directory, 'diskspace.log'), 'w') as f:
            for i in xrange(N):
                identifier=makeId(i)
                data=RECORD % i
                c.add(identifier=identifier, data=data)
                if i % 1000 == 0:
                    f.write("%s, %s\n" % (i, getSimpleDirSize(storeDir)))
                    print i
                sys.stdout.flush()

            c.commit()
            print 'committed'

            for j in xrange(M):
                i = randint(1, N)
                identifier=makeId(i)
                data=RECORD % j
                c.add(identifier=identifier, data=data)
                if j % 1000 == 0:
                    f.write("%s, %s\n" % (i, getSimpleDirSize(storeDir)))
                    print j, i
                    sleepWheel(1.0)
                if j % 10000 == 0:
                    t = time()
                    c.commit()
                    print 'commit took %s' % (time() - t)
                sys.stdout.flush()

            sleepWheel(2.0)

            t = time()
            c.close()
            print 'close took %s' % (time() - t)
            f.write("%s, %s\n" % (i, getSimpleDirSize(storeDir)))



def getSimpleDirSize(path):
    return sum(getsize(join(path, f)) for f in listdir(path) if isfile(join(path, f)))


RECORD = '''<?xml version="1.0" encoding="utf-8"?>
<rdf:RDF xmlns:dct="http://purl.org/dc/terms/"
  xmlns:owl="http://www.w3.org/2002/07/owl#" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
  xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#" xmlns:skos="http://www.w3.org/2004/02/skos/core#"
  xml:base="http://www.w3.org/2004/02/skos/core">
  <!-- This schema represents a formalisation of a subset of the semantic conditions
    described in the SKOS Reference document dated 18 August 2009, accessible
    at http://www.w3.org/TR/2009/REC-skos-reference-20090818/. XML comments of the form Sn are used to
    indicate the semantic conditions that are being expressed. Comments of the form
    [Sn] refer to assertions that are, strictly speaking, redundant as they follow
    from the RDF(S) or OWL semantics.

    A number of semantic conditions are *not* expressed formally in this schema. These are:

    S12
    S13
    S14
    S27
    S36
    S46

    For the conditions listed above, rdfs:comments are used to indicate the conditions.

   -->
  <owl:Ontology rdf:about="http://www.w3.org/2004/02/skos/core">
    <dct:title xml:lang="en">SKOS Vocabulary %s</dct:title>
    <dct:contributor>Dave Beckett</dct:contributor>
    <dct:contributor>Nikki Rogers</dct:contributor>
    <dct:contributor>Participants in W3C's Semantic Web Deployment Working Group.</dct:contributor>
    <dct:description xml:lang="en">An RDF vocabulary for describing the basic structure and content of concept schemes such as thesauri, classification schemes, subject heading lists, taxonomies, 'folksonomies', other types of controlled vocabulary, and also concept schemes embedded in glossaries and terminologies.</dct:description>
    <dct:creator>Alistair Miles</dct:creator>
    <dct:creator>Sean Bechhofer</dct:creator>
    <rdfs:seeAlso rdf:resource="http://www.w3.org/TR/skos-reference/"/>
  </owl:Ontology>
  <rdf:Description rdf:about="#Concept">
    <rdfs:label xml:lang="en">Concept</rdfs:label>
    <rdfs:isDefinedBy rdf:resource="http://www.w3.org/2004/02/skos/core"/>
    <skos:definition xml:lang="en">An idea or notion; a unit of thought.</skos:definition>
    <!-- S1 -->
    <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#Class"/>
  </rdf:Description>
  <rdf:Description rdf:about="#ConceptScheme">
    <rdfs:label xml:lang="en">Concept Scheme</rdfs:label>
    <rdfs:isDefinedBy rdf:resource="http://www.w3.org/2004/02/skos/core"/>
    <skos:definition xml:lang="en">A set of concepts, optionally including statements about semantic relationships between those concepts.</skos:definition>
    <skos:scopeNote xml:lang="en">A concept scheme may be defined to include concepts from different sources.</skos:scopeNote>
    <skos:example xml:lang="en">Thesauri, classification schemes, subject heading lists, taxonomies, 'folksonomies', and other types of controlled vocabulary are all examples of concept schemes. Concept schemes are also embedded in glossaries and terminologies.</skos:example>
    <!-- S2 -->
    <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#Class"/>
    <!-- S9 -->
    <owl:disjointWith rdf:resource="#Concept"/>
  </rdf:Description>
  <rdf:Description rdf:about="#Collection">
    <rdfs:label xml:lang="en">Collection</rdfs:label>
    <rdfs:isDefinedBy rdf:resource="http://www.w3.org/2004/02/skos/core"/>
    <skos:definition xml:lang="en">A meaningful collection of concepts.</skos:definition>
    <skos:scopeNote xml:lang="en">Labelled collections can be used where you would like a set of concepts to be displayed under a 'node label' in the hierarchy.</skos:scopeNote>
    <!-- S28 -->
    <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#Class"/>
    <!-- S37 -->
    <owl:disjointWith rdf:resource="#Concept"/>
    <!-- S37 -->
    <owl:disjointWith rdf:resource="#ConceptScheme"/>
  </rdf:Description>
  <rdf:Description rdf:about="#OrderedCollection">
    <rdfs:label xml:lang="en">Ordered Collection</rdfs:label>
    <rdfs:isDefinedBy rdf:resource="http://www.w3.org/2004/02/skos/core"/>
    <skos:definition xml:lang="en">An ordered collection of concepts, where both the grouping and the ordering are meaningful.</skos:definition>
    <skos:scopeNote xml:lang="en">Ordered collections can be used where you would like a set of concepts to be displayed in a specific order, and optionally under a 'node label'.</skos:scopeNote>
    <!-- S28 -->
    <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#Class"/>
    <!-- S29 -->
    <rdfs:subClassOf rdf:resource="#Collection"/>
  </rdf:Description>
  <rdf:Description rdf:about="#inScheme">
    <rdfs:label xml:lang="en">is in scheme</rdfs:label>
    <rdfs:isDefinedBy rdf:resource="http://www.w3.org/2004/02/skos/core"/>
    <skos:definition xml:lang="en">Relates a resource (for example a concept) to a concept scheme in which it is included.</skos:definition>
    <skos:scopeNote xml:lang="en">A concept may be a member of more than one concept scheme.</skos:scopeNote>
    <!-- S3 -->
    <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#ObjectProperty"/>
    <!-- S4 -->
    <rdfs:range rdf:resource="#ConceptScheme"/>
    <!-- For non-OWL aware applications -->
    <rdf:type rdf:resource="http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"/>
  </rdf:Description>
  <rdf:Description rdf:about="#hasTopConcept">
    <rdfs:label xml:lang="en">has top concept</rdfs:label>
    <rdfs:isDefinedBy rdf:resource="http://www.w3.org/2004/02/skos/core"/>
    <skos:definition xml:lang="en">Relates, by convention, a concept scheme to a concept which is topmost in the broader/narrower concept hierarchies for that scheme, providing an entry point to these hierarchies.</skos:definition>
    <!-- S3 -->
    <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#ObjectProperty"/>
    <!-- S5 -->
    <rdfs:domain rdf:resource="#ConceptScheme"/>
    <!-- S6 -->
    <rdfs:range rdf:resource="#Concept"/>
    <!-- S8 -->
    <owl:inverseOf rdf:resource="#topConceptOf"/>
    <!-- For non-OWL aware applications -->
    <rdf:type rdf:resource="http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"/>
  </rdf:Description>
  <rdf:Description rdf:about="#topConceptOf">
    <rdfs:label xml:lang="en">is top concept in scheme</rdfs:label>
    <rdfs:isDefinedBy rdf:resource="http://www.w3.org/2004/02/skos/core"/>
    <skos:definition xml:lang="en">Relates a concept to the concept scheme that it is a top level concept of.</skos:definition>
    <!-- S3 -->
    <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#ObjectProperty"/>
    <!-- S7 -->
    <rdfs:subPropertyOf rdf:resource="#inScheme"/>
    <!-- S8 -->
    <owl:inverseOf rdf:resource="#hasTopConcept"/>
    <!-- For non-OWL aware applications -->
    <rdf:type rdf:resource="http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"/>
    <rdfs:domain rdf:resource="#Concept"/>
    <rdfs:range rdf:resource="#ConceptScheme"/>
  </rdf:Description>
  <rdf:Description rdf:about="#prefLabel">
    <rdfs:label xml:lang="en">preferred label</rdfs:label>
    <rdfs:isDefinedBy rdf:resource="http://www.w3.org/2004/02/skos/core"/>
    <skos:definition xml:lang="en">The preferred lexical label for a resource, in a given language.</skos:definition>
    <!-- S10 -->
    <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#AnnotationProperty"/>
    <!-- S11 -->
    <rdfs:subPropertyOf rdf:resource="http://www.w3.org/2000/01/rdf-schema#label"/>
    <!-- S14 (not formally stated) -->
    <rdfs:comment xml:lang="en">A resource has no more than one value of skos:prefLabel per language tag, and no more than one value of skos:prefLabel without language tag.</rdfs:comment>
    <!-- S12 (not formally stated) -->
    <rdfs:comment xml:lang="en">The range of skos:prefLabel is the class of RDF plain literals.</rdfs:comment>
    <!-- S13 (not formally stated) -->
    <rdfs:comment xml:lang="en">skos:prefLabel, skos:altLabel and skos:hiddenLabel are pairwise
      disjoint properties.</rdfs:comment>
    <!-- For non-OWL aware applications -->
    <rdf:type rdf:resource="http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"/>
  </rdf:Description>
  <rdf:Description rdf:about="#altLabel">
    <rdfs:label xml:lang="en">alternative label</rdfs:label>
    <rdfs:isDefinedBy rdf:resource="http://www.w3.org/2004/02/skos/core"/>
    <skos:definition xml:lang="en">An alternative lexical label for a resource.</skos:definition>
    <skos:example xml:lang="en">Acronyms, abbreviations, spelling variants, and irregular plural/singular forms may be included among the alternative labels for a concept. Mis-spelled terms are normally included as hidden labels (see skos:hiddenLabel).</skos:example>
    <!-- S10 -->
    <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#AnnotationProperty"/>
    <!-- S11 -->
    <rdfs:subPropertyOf rdf:resource="http://www.w3.org/2000/01/rdf-schema#label"/>
    <!-- S12 (not formally stated) -->
    <rdfs:comment xml:lang="en">The range of skos:altLabel is the class of RDF plain literals.</rdfs:comment>
    <!-- S13 (not formally stated) -->
    <rdfs:comment xml:lang="en">skos:prefLabel, skos:altLabel and skos:hiddenLabel are pairwise disjoint properties.</rdfs:comment>
    <!-- For non-OWL aware applications -->
    <rdf:type rdf:resource="http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"/>
  </rdf:Description>
  <rdf:Description rdf:about="#hiddenLabel">
    <rdfs:label xml:lang="en">hidden label</rdfs:label>
    <rdfs:isDefinedBy rdf:resource="http://www.w3.org/2004/02/skos/core"/>
    <skos:definition xml:lang="en">A lexical label for a resource that should be hidden when generating visual displays of the resource, but should still be accessible to free text search operations.</skos:definition>
    <!-- S10 -->
    <rdf:type rdf:resource="http://www.w3.org/2002/07/owl#AnnotationProperty"/>
    <!-- S11 -->
    <rdfs:subPropertyOf rdf:resource="http://www.w3.org/2000/01/rdf-schema#label"/>
    <!-- S12 (not formally stated) -->
    <rdfs:comment xml:lang="en">The range of skos:hiddenLabel is the class of RDF plain literals.</rdfs:comment>
    <!-- S13 (not formally stated) -->
    <rdfs:comment xml:lang="en">skos:prefLabel, skos:altLabel and skos:hiddenLabel are pairwise disjoint properties.</rdfs:comment>
    <!-- For non-OWL aware applications -->
    <rdf:type rdf:resource="http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"/>
  </rdf:Description>
</rdf:RDF>'''


