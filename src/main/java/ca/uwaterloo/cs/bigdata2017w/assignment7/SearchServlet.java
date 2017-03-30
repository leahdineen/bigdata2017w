package ca.uwaterloo.cs.bigdata2017w.assignment7;

import java.io.IOException;
import java.lang.Exception;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.http.HttpStatus;
import java.util.Arrays;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;

public class SearchServlet extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) 
    throws ServletException, IOException {
    
    String query = request.getParameter("query");
    HBaseSearchEndpoint.retrieval_args[6] = "-query";
    HBaseSearchEndpoint.retrieval_args[7] = query;

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      System.setOut(new PrintStream(baos));
      BooleanRetrievalHBase.main(HBaseSearchEndpoint.retrieval_args);
    } catch (Exception e) {
      response.setStatus(HttpStatus.IM_A_TEAPOT_418);
      return;
    }

    response.setStatus(HttpStatus.OK_200);

    JSONArray retrieved = new JSONArray();

    String[] lines = baos.toString().split("[\\r\\n]+");
    // ignore first line (just telling you the query)
    // ignore last line (just telling you how long the query took)
    for(int i = 1; i < lines.length - 1; i++){
        String[] results = lines[i].split("[\\t]");

        JSONObject r = new JSONObject();
        r.put("docid", results[0]);
        r.put("text", results[1]);
        retrieved.add(r);
    }

    StringWriter out = new StringWriter();
    retrieved.writeJSONString(out);
      
    String jsonText = out.toString();

    response.getOutputStream().println(jsonText);
  }
}
