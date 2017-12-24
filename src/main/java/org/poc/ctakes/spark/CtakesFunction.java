/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.poc.ctakes.spark;

//Java imports
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.*;

//UIMA imports
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.pipeline.SimplePipeline;
import org.apache.uima.util.XMLSerializer;
import org.apache.uima.cas.FSIndex;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.Type;
import org.apache.uima.jcas.cas.FSArray;
import org.apache.uima.jcas.JCas;
import org.apache.uima.UIMAException;

//Apache Spark imports
import org.apache.spark.api.java.function.Function;

//Apache cTakes imports
import org.apache.ctakes.typesystem.type.refsem.OntologyConcept;
import org.apache.ctakes.typesystem.type.textsem.*;
import org.apache.ctakes.clinicalpipeline.ClinicalPipelineFactory;

/**
 * @author Yuriy Toropov
 *
 */
public class CtakesFunction implements Function<String, String> {

	transient JCas jcas = null;
	transient AnalysisEngineDescription aed = null;
	//transient AnalysisEngine ae = null;

	private void setup() throws UIMAException, java.net.MalformedURLException {
		System.setProperty("ctakes.umlsuser", "");
		System.setProperty("ctakes.umlspw", "");
		this.jcas = JCasFactory.createJCas();
		this.aed = ClinicalPipelineFactory.getDefaultPipeline();
		//this.ae = AnalysisEngineFactory.createEngine(ClinicalPipelineFactory.getDefaultPipeline());

	}

	private void readObject(ObjectInputStream in) {
		try {
			in.defaultReadObject();
			this.setup();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (UIMAException e) {
			e.printStackTrace();
		}
	}

	/*
	 * //Code to produce CAS output in XML form
	 * //or use XmiWriterCasConsumerCtakes class from org.apache.ctakes.core.cc
	 * AnalysisEngine xWriter = {return AnalysisEngineFactory.createEngine(
     *   XmiWriterCasConsumerCtakes.class,
     *   XmiWriterCasConsumerCtakes.PARAM_OUTPUTDIR,
     *   outputDirectory
     *   ); }
     *   SimplePipeline.runPipeline(
     *   collectionReader,
     *   aggregateBuilder.createAggregate(),
     *   xWriter); 
	 * @Override public String call(String paragraph) throws Exception {
	 * 
	 * this.jcas.setDocumentText(paragraph);
	 * 
	 * // final AnalysisEngineDescription aed = getFastPipeline(); 
	 * // Outputs from default and fast pipelines are identical 
	 * ByteArrayOutputStream baos = new ByteArrayOutputStream(); SimplePipeline.runPipeline(this.jcas, this.aed);
	 * XmiCasSerializer xmiSerializer = new XmiCasSerializer(jcas.getTypeSystem());
	 * XMLSerializer xmlSerializer = new XMLSerializer(baos, true);
	 * xmiSerializer.serialize(jcas.getCas(), xmlSerializer.getContentHandler());
	 * this.jcas.reset(); 
	 * return baos.toString("utf-8"); 
	 * }
	 */

	@Override
	//THIS IS FOR POC ONLY FOR NOW
	public String call(String paragraph) throws Exception {

		this.jcas.setDocumentText(paragraph);

		SimplePipeline.runPipeline(this.jcas, this.aed);
		//SimplePipeline.runPipeline(this.jcas, this.ae);
		FSIndex index = this.jcas.getAnnotationIndex(IdentifiedAnnotation.type);
		FSIterator iter = index.iterator();

		ArrayList<String> types = new ArrayList<String>();

		// only get the following types of annotations
		types.add("org.apache.ctakes.typesystem.type.textsem.SignSymptomMention");
		types.add("org.apache.ctakes.typesystem.type.textsem.DiseaseDisorderMention");
		types.add("org.apache.ctakes.typesystem.type.textsem.AnatomicalSiteMention");
		types.add("org.apache.ctakes.typesystem.type.textsem.ProcedureMention");
		types.add("org.apache.ctakes.typesystem.type.textsem.MedicationMention");
		HashSet<String> hsAnnotations = new HashSet<String>();

		String type = "";
		String completeResult = "";
		FSArray codesArray;

		while (iter.isValid()) {

			// TODO: Check if we correctly iterate over index
			// using https://uima.apache.org/d/uimaj-2.7.0/apidocs/org/apache/uima/cas/FSIterator.html documentation
			IdentifiedAnnotation annotation = (IdentifiedAnnotation) iter.get();
			type = annotation.getType().toString();
			if (types.contains(type)) {

				codesArray = annotation.getOntologyConceptArr();
				String[] codesStrings = new String[codesArray.size()];

				for (int i = 0; i < codesArray.size(); i++) {
					hsAnnotations.add(String.format("%s,%s", type,((OntologyConcept) codesArray.get(i)).getCode()));
				}
			}

			iter.moveToNext();
		}
	
		completeResult = String.join("|", hsAnnotations);
		this.jcas.reset();
		return completeResult;
	}
}
