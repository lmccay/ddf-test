package org.apache.knox.ddf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;

public class YamlUnitTest {
    private ObjectMapper mapper;

    @Before
    public void setup() {
        mapper = new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER));
        mapper.findAndRegisterModules();
    }

    @Test
    public void testDDFParsing() throws JsonParseException, JsonMappingException, IOException {
        Datalake datalake = mapper.readValue(new File("src/test/resources/ddf/ddf.yaml"), Datalake.class);
        assertEquals("cdp-ljm-datalake-924856-idbroker-assume-role", datalake.getDatalake_roles().get("IDBROKER_ROLE").get("iam_role"));
    }

    @Test
    public void testDDFRoleSelectionAlgorithm() throws JsonParseException, JsonMappingException, IOException {
        // create an instance of the datalake model to index and drive role selection
        Datalake datalake = mapper.readValue(new File("src/test/resources/ddf/ddf.yaml"), Datalake.class);

        // initializing the model creates various indexes upfront in order to more efficiently
        // interrogate the model for making role selection decisions.
        datalake.initialize();

        // the model will provide access to the metadata directly for things other than role selection as well
        // note that the iam_role element below is the name of the created IAM Role and will need to be reconciled
        // to an actual IAM Role ARN in order for STS.assumeRole to be called. This lookup is outside of the scope
        // of the model itself since it is cloud vendor specific.
        assertEquals("cdp-ljm-datalake-924856-idbroker-assume-role", datalake.getDatalake_roles().get("IDBROKER_ROLE").get("iam_role"));
        assertTrue(datalake.getDatalakeRolesForPath("/ljm-datalake-924856/ranger/audit").contains("RANGER_AUDIT_ROLE"));
        assertEquals(2, datalake.getDatalakeRolesForPath("/ljm-datalake-924856/ranger/audit").size());

        // retrieve storage paths so that storage location names can be associated with their
        // corresponding paths: /bucket/folder/... The paths are used to associate an incoming
        // request for credentials and the provided path to a set of datalake roles with some level
        // of access to the given path.
        Map<String, Map<String, String>> storage = datalake.getStorage();

       // test getting the rank for a permission
       // the ranking of permissions allows for the selection of the IAM Role with the greatest
       // level of access to a given location that the authenticated user can assume. While not
       // ideal in terms of least privilege, it does approximate the likely choice that a user
       // make if she were making this decision manually with client config rather well.
       assertEquals(1, datalake.getPermissionRank("storage", "full-access"));

       // getting the list of roles for a given path that are ranked by their strength allows for the
       // role selection algorithm to check whether each of the roles associated with the path is actually
       // assumable by the authenticated user - either by user or group mapping. Once a match is found that
       // rolename would be used to look up the actual IAM Role ARN and allow it be assumed by idbroker.
       // NOTE: the path provided here is an actual object within the designated bucket and folders.
       // The getRankedDatalakeRolesForPath method utilizes a best match approach to select the path with
       // the longest match as the most specific and appropriate mapping for the path. Otherwise, we would
       // need to have storage locations defined for every object which would never work.
       assertTrue(datalake.getRankedDatalakeRolesForPath("/ljm-datalake-924856/ranger/audit/ljm.log").contains("RANGER_AUDIT_ROLE"));
    }
}
