package FeatureTransformer.FeatureTransformer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

//http://www.baeldung.com/jackson-json-node-tree-model
//reference: http://www.baeldung.com/jackson-json-node-tree-model
//http://www.studytrails.com/java/json/jackson-create-json/
//https://www.mkyong.com/java/jackson-tree-model-example/
	
public class FeatureSelector {

	private static FeatureSelector instance;
	private final static Logger logger = Logger.getLogger(FeatureSelector.class);

	private FeatureSelector() {

	}

	// To avoid cost associated with the synchronized method after the instance
	// is created,
	// use double checked locking principle.
	public static FeatureSelector getInstance() {
		if (instance == null) {
			synchronized (FeatureSelector.class) {
				if (instance == null) {
					instance = new FeatureSelector();
				}
			}
		}
		return instance;
	}
	
	
	public String selectField(String jsonStr) {
		System.out.println("selectField from json:");
		System.out.print(jsonStr);
		System.out.println("!!!");
		
		/*
		 *{
		 * "BLTV": 80,
		 * "BaseLoanAmount": 394400.0,
		 * "ClosingCost": {
		 * 		"ClosingDisclosure1": {
		 * 			"PPC1EstimatedEscrowAmount": 351.54,
      				"PPC1MaximumMonthlyPayment": 2379.31,
      				"PPC1MinimumMonthlyPayment": 2379.31,
      			},
      			"ClosingDisclosure2": {
      				"BorrowerClosingCostAtClosing": 9666.83,
      				"TotalBorrowerPaidAtClosing": 3836.75,
      			},
    			"DisclosedSalesPrice": 499000.0,
    		}
    		DownPayment": {
		    "Amount": 104600.0,
		    "DownPaymentType": "RetirementFunds"
		  	},
		  	"DownPaymentPercent": 20.962
		  }	
		 */
		
		Map<String, String> mapData = new HashMap<>();
		JsonNode root;
		ObjectMapper objectMapper;
		objectMapper = new ObjectMapper();
		try {
			root = objectMapper.readTree(jsonStr);
			
			//Traverse a path
			//JsonNode locatedNode = rootNode.path("name").path("last");
					
			int val =  root.get("BLTV").asInt();
			mapData.put("bltv", String.valueOf(root.get("BLTV").asInt()));
			mapData.put("baseloan_amount", String.valueOf(root.get("BaseLoanAmount").asInt()));
			mapData.put("estimated_escrow_amount",
					String.valueOf(root.path("ClosingCost").path("ClosingDisclosure1").get("PPC1EstimatedEscrowAmount").asDouble()));
			mapData.put("down_payment",  String.valueOf(root.path("DownPayment").get("Amount")));
			
			
			ObjectMapper mapper = new ObjectMapper();
			String jsonResult = mapper.writerWithDefaultPrettyPrinter()
			  .writeValueAsString(mapData);

			return jsonResult;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return jsonStr;
		}

	}
	
	public String writeJson(JsonNode node) {
		
		return "";
	}


}
