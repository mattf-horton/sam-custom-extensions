package hortonworks.hdf.sam.custom.processor.stellar;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import junit.framework.Assert;
import org.apache.metron.stellar.common.StellarAssignment;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static hortonworks.hdf.sam.custom.processor.stellar.StellarCustomProcessor.*;

public class StellarCustomProcessorTest {

	protected static final Logger LOG = LoggerFactory.getLogger(StellarCustomProcessorTest.class);

	@Test
	public void testPreParse() {
		// make sure the stellarStatement string parses as expected
		String stmt;
		List<StellarAssignment> stellarStatements;

		stmt = "output1 := input1";
		stellarStatements = new ArrayList<>();
		preParse(stmt, stellarStatements);
		Assert.assertEquals(1, stellarStatements.size());
		Assert.assertEquals("output1", stellarStatements.get(0).getVariable());
		Assert.assertEquals("input1", stellarStatements.get(0).getStatement());

		stmt = "output1 := input1 + input2\n input2 := 20 + 5";
		stellarStatements = new ArrayList<>();
		preParse(stmt, stellarStatements);
		Assert.assertEquals(2, stellarStatements.size());
		Assert.assertEquals("output1", stellarStatements.get(0).getVariable());
		Assert.assertEquals("input1 + input2", stellarStatements.get(0).getStatement());
		Assert.assertEquals("input2", stellarStatements.get(1).getVariable());
		Assert.assertEquals("20 + 5", stellarStatements.get(1).getStatement());

		stmt = "output1 := input1 + input2; input2 := 20 + 5";
		stellarStatements = new ArrayList<>();
		preParse(stmt, stellarStatements);
		Assert.assertEquals(2, stellarStatements.size());
		Assert.assertEquals("output1", stellarStatements.get(0).getVariable());
		Assert.assertEquals("input1 + input2", stellarStatements.get(0).getStatement());
		Assert.assertEquals("input2", stellarStatements.get(1).getVariable());
		Assert.assertEquals("20 + 5", stellarStatements.get(1).getStatement());

	}

	private void runStellarTestcase(
						String stellarStatements,
						String enrichedOutputFields,
						String projectionRemovedFields,
						Map<String, Object> inputFields,
						Map<String, Object> expectedOutputFields) throws ConfigException, ProcessingException
	{
		Map<String, Object> processorConfig = new HashMap<>();
		if (stellarStatements != null)
			processorConfig.put(CONFIG_STELLAR_STATEMENTS, stellarStatements);
		if (enrichedOutputFields != null)
			processorConfig.put(CONFIG_ENRICHED_OUTPUT_FIELDS, enrichedOutputFields);
		if (projectionRemovedFields != null)
			processorConfig.put(CONFIG_PROJECTION_REMOVED_FIELDS, projectionRemovedFields);

		StreamlineEvent inputEvent = new StreamlineEventImpl(inputFields, "abc123");
		StellarCustomProcessor scp = new StellarCustomProcessor();
		scp.validateConfig(processorConfig);
		scp.initialize(processorConfig);
		StreamlineEvent outputEvent = scp.process(inputEvent).get(0);
		Map<String, Object> outputFields = new HashMap<>(outputEvent);
		Assert.assertEquals(expectedOutputFields , outputFields);
	}

	@Test
	public void testReadWrite() throws ConfigException, ProcessingException {

		String stmt = "output1 := input1";
		String enrichedOutputFields = "output1";

		Map<String, Object> inputFields = new HashMap<>();
		final int input1 = 100;
		inputFields.put("input1", input1);

		Map<String, Object> expectedFields = new HashMap<>();
		expectedFields.put("input1", input1);
		expectedFields.put("output1", input1);
		runStellarTestcase(stmt, enrichedOutputFields, null,
						inputFields, expectedFields);
	}

	@Test
	public void testRemove() throws ConfigException, ProcessingException {

		String stmt = "output1 := input1 + input2";
		String enrichedOutputFields = "output1";
		String projectionRemovedFields = "input1";

		Map<String, Object> inputFields = new HashMap<>();
		final int input1 = 100;
		inputFields.put("input1", input1);
		final int input2 = 50;
		inputFields.put("input2", input2);

		Map<String, Object> expectedFields = new HashMap<>();
		expectedFields.put("input2", input2);
		expectedFields.put("output1", 150);
		runStellarTestcase(stmt, enrichedOutputFields, projectionRemovedFields,
						inputFields, expectedFields);
	}

	@Test
	public void testNonPersist() throws ConfigException, ProcessingException {

		String stmt = "output1 := input1 + input2\n input2 := 20";
		String enrichedOutputFields = "output1";
		String projectionRemovedFields = "input1";

		Map<String, Object> inputFields = new HashMap<>();
		int input1 = 100;
		inputFields.put("input1", input1);
		int input2 = 50;
		inputFields.put("input2", input2);

		Map<String, Object> expectedFields = new HashMap<>();
		expectedFields.put("input2", 50);
		expectedFields.put("output1", 150);
		runStellarTestcase(stmt, enrichedOutputFields, projectionRemovedFields,
						inputFields, expectedFields);
	}

	@Test
	public void testPersist() throws ConfigException, ProcessingException {

		String stmt = "output1 := input1 + input2\n input2 := 20";
		String enrichedOutputFields = "output1, input2";
		String projectionRemovedFields = "input1";

		Map<String, Object> inputFields = new HashMap<>();
		int input1 = 100;
		inputFields.put("input1", input1);
		int input2 = 50;
		inputFields.put("input2", input2);

		Map<String, Object> expectedFields = new HashMap<>();
		expectedFields.put("input2", 20);
		expectedFields.put("output1", 150);
		runStellarTestcase(stmt, enrichedOutputFields, projectionRemovedFields,
						inputFields, expectedFields);
	}

}
