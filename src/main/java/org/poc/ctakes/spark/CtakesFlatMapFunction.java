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
import javax.swing.event.ListSelectionEvent;
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
import org.apache.spark.api.java.function.FlatMapFunction;

//Apache cTakes imports
import org.apache.ctakes.typesystem.type.refsem.OntologyConcept;
import org.apache.ctakes.typesystem.type.textsem.*;
import org.apache.ctakes.clinicalpipeline.ClinicalPipelineFactory;

/**
 * @author Yuriy Toropov
 *
 */
//<T,R>
public class CtakesFlatMapFunction implements FlatMapFunction<java.util.Iterator<String>, String> { 

	transient JCas jcas = null;
	transient AnalysisEngine ae = null;

	private void setup() throws UIMAException, java.net.MalformedURLException {
		System.setProperty("ctakes.umlsuser", "");
		System.setProperty("ctakes.umlspw", "");
		this.jcas = JCasFactory.createJCas();
		this.ae = AnalysisEngineFactory.createEngine(ClinicalPipelineFactory.getDefaultPipeline());

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

	@Override
	//return table csv-like output of AnnotationType,code
	public java.util.Iterator<String> call(java.util.Iterator<String> partition) throws Exception {
 
		CtakesMapFunctionInner analyze=new CtakesMapFunctionInner();
		HashSet<String> analyzed=new HashSet<String>();
		while(partition.hasNext()) {
	
			String note=(String)partition.next();
			analyzed.addAll(analyze.call(note));
			
		}
		
		return analyzed.iterator();
	}
	
	//this could be done instead as an anonymous function using new Function<String,HashSet<String>>() {...}
	class CtakesMapFunctionInner implements Function<String,HashSet<String>> {

		@Override
		public HashSet<String> call(String paragraph) throws Exception {

			CtakesFlatMapFunction.this.jcas.setDocumentText(paragraph);

			//
			SimplePipeline.runPipeline(CtakesFlatMapFunction.this.jcas, CtakesFlatMapFunction.this.ae);
			
			FSIndex index = CtakesFlatMapFunction.this.jcas.getAnnotationIndex(IdentifiedAnnotation.type);
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
			FSArray codesArray;

			while (iter.isValid()) {

				// TODO: Check if we correctly iterate over index
				// using https://uima.apache.org/d/uimaj-2.7.0/apidocs/org/apache/uima/cas/FSIterator.html documentation
				IdentifiedAnnotation annotation = (IdentifiedAnnotation) iter.get();
				type = annotation.getType().toString();
				if (types.contains(type)) {

					codesArray = annotation.getOntologyConceptArr();
					
					for (int i = 0; i < codesArray.size(); i++) {
						hsAnnotations.add(String.format("%s,%s", type,((OntologyConcept) codesArray.get(i)).getCode()));
					}
				}

				iter.moveToNext();
			}

			CtakesFlatMapFunction.this.jcas.reset();
			return hsAnnotations;
		}
	}
}
