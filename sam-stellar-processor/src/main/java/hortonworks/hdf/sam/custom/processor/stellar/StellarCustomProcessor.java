/**
 * Copyright 2017 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package hortonworks.hdf.sam.custom.processor.stellar;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;
import org.apache.commons.lang.StringUtils;
import org.apache.metron.stellar.common.DefaultStellarStatefulExecutor;
import org.apache.metron.stellar.common.StellarAssignment;
import org.apache.metron.stellar.common.StellarProcessor;
import org.apache.metron.stellar.dsl.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Pattern;

/**
 * A Stellar based processor for Streamline that can do enrichment and projection.
 *
 * The processor will execute a user-provided sequence of Stellar assignment
 * statements.  At the end, results will be mapped to field names provided by
 * the user in the enrichedOutputFields arg. These may include existing input
 * field names, and newly created field names. Fields no longer needed due to
 * projection mapping will be removed; these fields are specified by the user
 * in the projectionRemovedFields arg.
 *
 * All of the input schema fields plus the enrichedOutputFields are available
 * to the Stellar context as named variables.  Output field names not defined
 * in the input schema will be initialized to null. Missing input fields expected
 * in the input schema will be initialized to null.  Assignment to fields named in
 * the enrichedOutputFields will be persisted into the output event.  Assignments
 * to fields not named in enrichedOutputFields will be valid during the execution
 * of the Stellar statements (possibly masking original input field values), but
 * will not be persisted into the output event.  Variables with null values
 * at the end of execution will not be persisted into the output event, unless
 * they were already present as nulls in the input event.
 *
 * The syntax of the Stellar assignment statements is very specific.  They may
 * only be of the form:
 *     var_name := stellar_expression
 * where var_name must have no whitespace, and the stellar_expression may be of any
 * length but must be on one logical line and must not include further assignments.
 * The logical line is extended across multiple physical lines if the last
 * non-white-space character in a line is a backslash.  A newline not preceded by
 * a backslash terminates the logical line.  Continuation lines may start with an
 * indent, but the indent whitespace will be stripped.  If separating whitespace
 * is desired, put it before the backslash on the earlier line.
 *
 *     TEMPORARY HACK: Because the current SAM GUI doesn't provide editable text windows
 *     for config input, only Strings, we allow a semicolon ';' to act as linebreak.
 *     In reality, semicolon is not a recognized Stellar syntax element.
 *
 * The constraint against assignment statements inside of other Stellar expressions
 * may be relaxed in the future, depending on the evolution of Stellar.
 *
 * The user is strongly encouraged to test and debug the Stellar statements
 * in the REPL, where debugging is much easier, then copy-and-paste them into
 * the StellarCustomProcessor.
 *
 * No state is persisted between event executions (other than base Stellar
 * configuration and the cached parse of the Stellar expressions).
 *
 * TODO: a comment about type and type conversion of input fields?
 * NOTE: Could add an "alias..as" functionality, if needed to rename fields
 * temporarily during Stellar execution (e.g. to avoid name collisions).
 * NOTE: someday add auto-complete while typing into the GUI
 * stellarStatement field, and/or a Console sub-window in the config GUI.
 *
 * Examples of Stellar statement include:
 *
 * 1. maxIssue := if issue1 < issue2 then issue2 else issue1
 * 
 * 2. isLocalIP := IN_SUBNET(orig_ip_addr, "192.168.0.0/16")
 *
 */
public class StellarCustomProcessor implements CustomProcessorRuntime {

	protected static final Logger LOG = LoggerFactory
			.getLogger(hortonworks.hdf.sam.custom.processor.stellar.StellarCustomProcessor.class);

	static final String CONFIG_STELLAR_STATEMENTS = "stellarStatements";
	static final String CONFIG_ENRICHED_OUTPUT_FIELDS = "enrichedOutputFields";
	static final String CONFIG_PROJECTION_REMOVED_FIELDS = "projectionRemovedFields";

  private List<StellarAssignment> stellarStatements;
	private Set<String> enrichedOutputFields;
	private Set<String> projectionRemovedFields;


	public void cleanup() {
	}
	

	/**
	 * Initialize the processor from configuration
	 */
	public void initialize(Map<String, Object> config) {
		LOG.info("Initializing + " + hortonworks.hdf.sam.custom.processor.stellar.StellarCustomProcessor.class.getName());

		String stellarText = (String) config.getOrDefault(
		        CONFIG_STELLAR_STATEMENTS, "");
    this.stellarStatements = new ArrayList<>();
    preParse(stellarText, stellarStatements);
		LOG.info("The configured Stellar statements are: " + stellarStatements);

		String outputFields = (String) config.getOrDefault(
		        CONFIG_ENRICHED_OUTPUT_FIELDS, "");
		String outputFieldsCleaned = StringUtils.deleteWhitespace(outputFields);
    if (outputFieldsCleaned.length() == 0) {
      this.enrichedOutputFields = new HashSet<String>(0);
    }
    else {
      this.enrichedOutputFields = new HashSet<String>(Arrays.asList(
              outputFieldsCleaned.split(",")));
    }
		LOG.info("Enriched Output fields are: " + enrichedOutputFields);

    String removedFields = (String) config.getOrDefault(
		        CONFIG_PROJECTION_REMOVED_FIELDS, "");
		String removedFieldsCleaned = StringUtils.deleteWhitespace(removedFields);
    if (removedFieldsCleaned.length() == 0) {
      this.projectionRemovedFields = new HashSet<String>(0);
    }
    else {
      this.projectionRemovedFields = new HashSet<String>(Arrays.asList(
              removedFieldsCleaned.split(",")));
    }
		LOG.info("Projection Removed fields are: " + projectionRemovedFields);

		//add here if needed: read and set Stellar config values into stellarContext.

	}

	/*
	 * Break the Stellar Statements field into an ordered list of StellarAssignments,
	 * each consisting of assignment variable name and executable expression.
	 * The Stellar expressions that use continuation lines are glued together here.
	 * Blank lines are ignored.
	 *
	 * Returns false if preParse error, otherwise true.
	 */
	static boolean preParse(String stellarText,
                        List<StellarAssignment> stellarStatements) {
    if (stellarText == null) return true;
	  StringBuilder partialLine = new StringBuilder();
	  Pattern.compile("$|;", Pattern.MULTILINE)
            .splitAsStream(stellarText)
            .map(line -> line.trim())
            .forEachOrdered(line -> {
              if (line.length() > 0) {
                if (line.endsWith("\\")) {
                  partialLine.append(line.substring(0, line.length() - 1));
                } else if (partialLine.length() > 0) {
                  partialLine.append(line);
                  stellarStatements.add(
                          StellarAssignment.from(
                                  partialLine.toString()));
                  partialLine.setLength(0);
                } else {
                  stellarStatements.add(
                          StellarAssignment.from(line));
                }
              }
            });
    if (partialLine.length() > 0) {
      LOG.warn("Last line in " + CONFIG_STELLAR_STATEMENTS + " ended with backslash");
      stellarStatements.add(
              StellarAssignment.from(
                      partialLine.toString()));
      return false;
    }
    return true;
  }


	/**
	 * Enrich the event with results from the Stellar statements,
   * while removing any unneeded fields that are "collapsed" by projection.
	 */
	public List<StreamlineEvent> process(StreamlineEvent event)
			throws ProcessingException {
		LOG.info("Event about to be processed: [" + event + "]");

		//copy the original state of all fields not to be modified
    Map<String, Object> eventFields = new HashMap<>(event);
    Map<String, Object> immutableEventFields = new HashMap<>();
    for (String k : eventFields.keySet()) {
      if (!enrichedOutputFields.contains(k) && !projectionRemovedFields.contains(k)) {
        immutableEventFields.put(k, eventFields.get(k));
      }
    }
    StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
    builder.putAll(immutableEventFields);

		/* Enrich */
		try {
      eventFields = executeList(stellarStatements, eventFields);
    } catch (ParseException rte) {
		  throw new ProcessingException("Stellar parse/exec exception", rte);
    }
    Map<String, Object> enrichValues = new HashMap<>(enrichedOutputFields.size());
    for (String k : enrichedOutputFields) {
      Object v = eventFields.get(k);
      if (v != null) {
        enrichValues.put(k,v);
      }
    }
		LOG.info("Enriching event [" + event
				+ "]  with the following enriched values: " + enrichValues);
    builder.putAll(enrichValues);

		/* Build the enriched streamline event and return */
    StreamlineEvent enrichedEvent = builder.dataSourceId(
				event.getDataSourceId()).build();
		LOG.info("Enriched StreamLine Event is: " + enrichedEvent);

		List<StreamlineEvent> newEvents = Collections
				.<StreamlineEvent> singletonList(enrichedEvent);

		return newEvents;
	}

  /**
   * Execute a list of Stellar assignments.
   *
   * @param stellarStatements     The assignment statements to execute.
   * @param variableState     The map containing the initial state of variables
   *                          from the message input fields.
   * @return the enriched map containing final state of variables as assigned
   *         by the stellarStatements, as an ImmutableMap (although it is
   *         declared as simply a Map).
   */
  private Map<String, Object> executeList(
              List<StellarAssignment> stellarStatements,
              Map<String, Object> variableState) {

    DefaultStellarStatefulExecutor stellarStatefulExecutor =
            new DefaultStellarStatefulExecutor(variableState);

    for (StellarAssignment a : stellarStatements) {
      stellarStatefulExecutor.assign(a.getVariable(), a.getStatement(), null);
    }
    return stellarStatefulExecutor.getState();
  }


	@Override
	public void validateConfig(Map<String, Object> config) throws ConfigException {

    String outputFields = (String) config.getOrDefault(
            CONFIG_ENRICHED_OUTPUT_FIELDS, "");
    String outputFieldsCleaned = StringUtils.deleteWhitespace(outputFields);
    List<String> enrichedOutputFieldsList;
    if (outputFieldsCleaned.length() == 0) {
      enrichedOutputFieldsList = Arrays.asList(new String[0]);
    }
    else {
      enrichedOutputFieldsList = Arrays.asList(
              outputFieldsCleaned.split(","));
    }

    String removedFields = (String) config.getOrDefault(
            CONFIG_PROJECTION_REMOVED_FIELDS, "");
    String removedFieldsCleaned = StringUtils.deleteWhitespace(removedFields);
    List<String> projectionRemovedFieldsList;
    if (removedFieldsCleaned.length() == 0) {
      projectionRemovedFieldsList = Arrays.asList(new String[0]);
    }
    else {
      projectionRemovedFieldsList = Arrays.asList(
              removedFieldsCleaned.split(","));
    }

    if (!Collections.disjoint(enrichedOutputFieldsList, projectionRemovedFieldsList)) {
      throw new ConfigException("" + CONFIG_ENRICHED_OUTPUT_FIELDS + " and "
              + CONFIG_PROJECTION_REMOVED_FIELDS
              + " must not contain any of the same field names.");
    }
    if (enrichedOutputFieldsList.contains("")) {
      throw new ConfigException("Empty string occurs in "+CONFIG_ENRICHED_OUTPUT_FIELDS);
    }
    if (projectionRemovedFieldsList.contains("")) {
      throw new ConfigException("Empty string occurs in "+CONFIG_PROJECTION_REMOVED_FIELDS);
    }

    String stellarText = (String) config.getOrDefault(
            CONFIG_STELLAR_STATEMENTS, "");
    List<StellarAssignment> stellarStatementsList = new ArrayList<>();
    if (!preParse(stellarText, stellarStatementsList)) {
      throw new ConfigException("Last line in " + CONFIG_STELLAR_STATEMENTS + " ended with backslash");
    }
    for (int i=0; i<stellarStatementsList.size(); i++) {
      if (stellarStatementsList.get(i).getStatement().contains(":=")) {
        throw new ConfigException(String.format(
                "Statement %d of %d in %s contains assignment (':=') in rhs expression;\n"
                + "unallowed at this time.  Bad rhs expression: \n%s"
                , i, stellarStatementsList.size(), CONFIG_STELLAR_STATEMENTS
                , stellarStatementsList.get(i).getStatement()));
      }
    }

    StellarProcessor processor = new StellarProcessor();
    try {
      for (StellarAssignment a : stellarStatementsList) {
        processor.validate(a.getStatement());
      }
    } catch (ParseException pe) {
      throw new ConfigException("Stellar expression failed to parse cleanly, with all variables null.", pe);
    }
  }

}
