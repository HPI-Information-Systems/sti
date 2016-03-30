package uk.ac.shef.dcs.kbsearch.freebase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import uk.ac.shef.dcs.kbsearch.rep.Attribute;
import uk.ac.shef.dcs.kbsearch.rep.Clazz;
import uk.ac.shef.dcs.util.CollectionUtils;
import uk.ac.shef.dcs.util.StringUtils;


/**
 * Created with IntelliJ IDEA.
 * User: zqz
 * Date: 18/01/14
 * Time: 22:06
 * To change this template use File | Settings | File Templates.
 */
public class FreebaseQueryHelper {


    public static Logger LOG = Logger.getLogger(FreebaseQueryHelper.class.getName());
    //private String BASE_QUERY_URL="https://www.googleapis.com/freebase/v1/mqlread";
    private JSONParser jsonParser;
    private FreebaseQueryInterrupter interrupter;
    private HttpTransport httpTransport;
    private HttpRequestFactory requestFactory;
    private Properties properties;

    private static final String FB_MAX_QUERY_PER_SECOND="fb.query.max.sec";

    private static final String FB_MAX_QUERY_PER_DAY="fb.query.max.day";

    private static final String FB_QUERY_API_URL_TOPIC ="fb.query.apiurl.topic";

    private static final String FB_QUERY_API_URL_SEARCH ="fb.query.apiurl.search";

    private static final String FB_QUERY_API_URL_MQL ="fb.query.apiurl.mql";

    private static final String FB_QUERY_API_KEY="fb.query.api.key";

    private static final String FB_HOMEPAGE="fb.homepage";

    private static final String FB_QUERY_PARAM_LIMIT="fb.query.param.limit";

    public FreebaseQueryHelper(Properties properties) throws IOException {
        this.properties=properties;
        interrupter = new FreebaseQueryInterrupter(Integer.valueOf(properties.get(FB_MAX_QUERY_PER_SECOND).toString()),
                Integer.valueOf(properties.get(FB_MAX_QUERY_PER_DAY).toString()));
        httpTransport = new NetHttpTransport();
        requestFactory = httpTransport.createRequestFactory();
        jsonParser = new JSONParser();
    }


    //given a topic id, returns its facts. result is a list of string array, where in each array, value 0 is the property name; value 1 is the value;
    //value 2 could be null or a string, when it is an id of a topic (if null then value of property is not topic); value 4 is "y" or "n", meaning if
    //if the property is a nested property of the topic of interest.
    public List<Attribute> topicapi_getAttributesOfTopic(String id) throws IOException {
        Date start = new Date();
        List<Attribute> res = new ArrayList<>();
        GenericUrl url = new GenericUrl(properties.get(FB_QUERY_API_URL_TOPIC).toString() + id);
        url.put("key", properties.get(FB_QUERY_API_KEY));
        url.put("limit", 100);
        HttpRequest request = requestFactory.buildGetRequest(url);
        HttpResponse httpResponse = interrupter.executeQuery(request, true);
        try {
            JSONObject topic = (JSONObject) jsonParser.parse(httpResponse.parseAsString());
            JSONObject properties = (JSONObject) topic.get("property");
            parsePropertiesOfTopicAPI(properties, res, false);
        } catch (ParseException pe) {
            pe.printStackTrace();
        }
        LOG.info("\tQueryFreebase:" + (new Date().getTime() - start.getTime()));
        return res;

    }

    public List<Attribute> topicapi_getTypesOfTopicID(String id) throws IOException {
        Date start = new Date();
        List<Attribute> res = new ArrayList<>();
        GenericUrl url = new GenericUrl(properties.get(FB_QUERY_API_URL_TOPIC).toString() + id);
        url.put("key", properties.get(FB_QUERY_API_KEY));
        url.put("filter", "/type/object/type");
        HttpRequest request = requestFactory.buildGetRequest(url);
        HttpResponse httpResponse = interrupter.executeQuery(request, true);
        try {
            JSONObject topic = (JSONObject) jsonParser.parse(httpResponse.parseAsString());
            JSONObject properties = (JSONObject) topic.get("property");
            if(properties!=null)
                parsePropertiesOfTopicAPI(properties, res, false);
        } catch (ParseException pe) {
            pe.printStackTrace();
        }
        LOG.info("\tQueryFreebase:" + (new Date().getTime() - start.getTime()));
        return res;

    }

    //[] = {property, val.toString(), id.toString(), nested_flag}  (nested flag=y/n)
    public List<Attribute> topicapi_getFactsOfTopicID(String id, String filter) throws IOException {
        Date start = new Date();
        List<Attribute> res = new ArrayList<>();
        GenericUrl url = new GenericUrl(properties.get(FB_QUERY_API_URL_TOPIC).toString() + id);
        url.put("key", properties.get(FB_QUERY_API_KEY));
        url.put("filter", filter);
        url.put("limit", 200);
        HttpRequest request = requestFactory.buildGetRequest(url);
        HttpResponse httpResponse = interrupter.executeQuery(request, true);
        try {
            JSONObject topic = (JSONObject) jsonParser.parse(httpResponse.parseAsString());
            JSONObject properties = (JSONObject) topic.get("property");
            parsePropertiesOfTopicAPI(properties, res, false);
        } catch (ParseException pe) {
            pe.printStackTrace();
        }
        LOG.info("\tQueryFreebase:" + (new Date().getTime() - start.getTime()));
        return res;

    }

    private void parsePropertiesOfTopicAPI(JSONObject json, List<Attribute> out, boolean nested) {
        /*if(json==null)
            System.out.println();*/
        Iterator<String> prop_keys = json.keySet().iterator();
        while (prop_keys.hasNext()) {
            String prop = prop_keys.next();
            try {
                JSONObject propValueObj = (JSONObject) json.get(prop);
                JSONArray jsonArray = (JSONArray) propValueObj.get("values");
                Object c = propValueObj.get("valuetype");
                if (c != null && c.toString().equals("compound"))
                    parsePropertyValues(jsonArray, prop, out, nested, true);
                else
                    parsePropertyValues(jsonArray, prop, out, nested, false);
            } catch (Exception e) {
            }
        }
    }

    private FreebaseTopic parseProperties_of_searchapi(JSONObject json) {
        FreebaseTopic obj = new FreebaseTopic(json.get("mid").toString());
        Object o = json.get("mid");
        if (o != null)
            obj.setId(o.toString());
        obj.setLabel(json.get("name").toString());
        obj.setScore(Double.valueOf(json.get("score").toString()));

        obj.setLanguage(json.get("lang").toString());
        return obj;
    }

    private void parsePropertyValues(JSONArray json, String property, List<Attribute> out, boolean nested, boolean skipCompound) {
        Iterator entry = json.iterator();
        Object val = null, id = null, mid = null, more_props = null;
        while (entry.hasNext()) {
            JSONObject key = (JSONObject) entry.next();
            if (skipCompound) {
                more_props = key.get("property");
                if (more_props != null)
                    parsePropertiesOfTopicAPI((JSONObject) more_props, out, true);
                continue;
            }

            val = key.get("text");
            if (property.equals("/common/topic/description") || property.equals("/common/document/text")) {
                Object changeVal = key.get("value");
                if (changeVal != null)
                    val = changeVal;
            }
            id = key.get("id");
            mid = key.get("mid");
            if (id == null && mid != null) id = mid;
            Attribute attr = new Attribute(property, val.toString());
            attr.setIsDirect(nested);
            if (val != null && id != null) {
                attr.setValueURI(id.toString());
                out.add(attr);
            }
            else if (val != null) {
                out.add(attr);
            }
        }

    }


    //operator - any means or; all means and
    public List<FreebaseTopic> searchapi_getTopicsByNameAndType(String name, String operator, boolean tokenMatch, int maxResult, String... types) throws IOException {
        Set<String> query_tokens = new HashSet<>();
        for (String t : name.split("[\\s+/\\-,]")) {
            t = t.trim();
            if (t.length() > 0)
                query_tokens.add(t);
        }

        Date start = new Date();
        HttpTransport httpTransport = new NetHttpTransport();
        HttpRequestFactory requestFactory = httpTransport.createRequestFactory();
        List<FreebaseTopic> res = new ArrayList<>();

        GenericUrl url = new GenericUrl(properties.get(FB_QUERY_API_URL_SEARCH).toString());
        url.put("query", name);
        url.put("limit", 20);
        url.put("prefixed", true);
        url.put("key", properties.get(FB_QUERY_API_KEY));

        StringBuilder filter = new StringBuilder();
        for (String t : types) {
            filter.append("type:").append(t).append(" ");
        }

        if (filter.length() > 0)
            url.put("filter", "(" + operator + " " + filter.toString().trim() + ")");

        HttpRequest request = requestFactory.buildGetRequest(url);
        HttpResponse httpResponse = interrupter.executeQuery(request, true);
        JSONObject response;
        try {
            response = (JSONObject) jsonParser.parse(httpResponse.parseAsString());
            JSONArray results = (JSONArray) response.get("result");
            int count = 0;
            for (Object result : results) {
                FreebaseTopic top = parseProperties_of_searchapi((JSONObject) result);

                if (count < maxResult) {
                    if (tokenMatch) {
                        Set<String> candidate_tokens = new HashSet<String>();

                        for (String t : top.getLabel().split("[\\s+/\\-,]")) {
                            t = t.trim();
                            if (t.length() > 0)
                                candidate_tokens.add(t);
                        }
                        candidate_tokens.retainAll(query_tokens);
                        if (candidate_tokens.size() > 0)
                            res.add(top);
                    } else
                        res.add(top);
                }

                //print or save this id
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }

        LOG.info("\tQueryFreebase:" + (new Date().getTime() - start.getTime()));
        return res;
    }


    public List<FreebaseTopic> mql_topics_with_name(int maxResults, String name, String operator, String... types) throws IOException {
        Set<String> query_tokens = new HashSet<String>();
        for (String t : name.split("\\s+")) {
            t = t.trim();
            if (t.length() > 0)
                query_tokens.add(t);
        }

        Date start = new Date();
        HttpTransport httpTransport = new NetHttpTransport();
        HttpRequestFactory requestFactory = httpTransport.createRequestFactory();
        List<FreebaseTopic> res = new ArrayList<FreebaseTopic>();

        final Map<FreebaseTopic, Double> candidates = new HashMap<FreebaseTopic, Double>();
        int limit = 20;
        int iterations = maxResults % limit;
        iterations = iterations == 0 ? maxResults / limit : maxResults / limit + 1;
        String cursorPoint = "";
        for (int i = 0; i < iterations; i++) {
            String query = "[{\"mid\":null," +
                    "\"name\":null," +
                    "\"name~=\":\"" + name + "\"," +
                    "\"/type/object/type\":[],";
            if (types.length > 0) {
                if (operator.equals("any")) {
                    query = query + "\"type|=\":[";
                    for (String t : types) {
                        query = query + "\"" + t + "\",";
                    }
                    if (query.endsWith(","))
                        query = query.substring(0, query.length() - 1).trim();
                    query = query + "],";
                } else if (operator.equals("and")) {
                    for (int n = 0; n < types.length; n++) {
                        String t = types[n];
                        if (n == 0)
                            query = query + "\"type\":\"" + t + "\",";
                        else
                            query = query + "\"and:type\":\"" + t + "\",";
                    }
                }
            }

            query = query +
                    "\"limit\":" + limit + "" +
                    "}]";

            GenericUrl url = new GenericUrl(properties.get(FB_QUERY_API_URL_MQL).toString());
            url.put("query", query);
            url.put("key", properties.get(FB_QUERY_API_KEY));
            url.put("cursor", cursorPoint);

            HttpRequest request = requestFactory.buildGetRequest(url);
            HttpResponse httpResponse = interrupter.executeQuery(request, true);
            System.out.print(limit * (i + 1));
            JSONObject response;
            try {
                response = (JSONObject) jsonParser.parse(httpResponse.parseAsString());
                cursorPoint = response.get("cursor").toString();
                JSONArray results = (JSONArray) response.get("result");

                for (Object result : results) {
                    JSONObject obj = (JSONObject) result;
                    String id = obj.get("mid").toString();
                    String e_name = obj.get("name").toString();
                    FreebaseTopic ent = new FreebaseTopic(id);
                    ent.setLabel(e_name);
                    if (obj.get("/type/object/type") != null) {
                        JSONArray jsonArray = (JSONArray) obj.get("/type/object/type");
                        for (int n = 0; n < jsonArray.size(); n++) {
                            String the_type = jsonArray.get(n).toString();
                            if (!the_type.equals("/common/topic") && !the_type.startsWith("/user/"))
                                ent.addType(new Clazz(the_type, the_type));
                        }
                    }
                    List<String> bow_ent = StringUtils.toBagOfWords(e_name, true, true, false);
                    List<String> bow_query = StringUtils.toBagOfWords(name, true, true,false);
                    int intersection = CollectionUtils.intersection(bow_ent, bow_query);
                    candidates.put(ent, ((double) intersection / bow_ent.size() + (double) intersection / bow_query.size()) / 2.0);
                    //print or save this id
                }

                if (results.size() < limit) {
                    break;
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }

        }

        LOG.info("\tQueryFreebase:" + (new Date().getTime() - start.getTime()));
        res.addAll(candidates.keySet());
        Collections.sort(res, (o1, o2) -> candidates.get(o2).compareTo(candidates.get(o1)));
        return res;
    }

    public List<String> mqlapi_topic_mids_with_wikipedia_pageid(String wikipedia_pageid) throws IOException {
        Date start = new Date();
        httpTransport = new NetHttpTransport();
        requestFactory = httpTransport.createRequestFactory();
        List<String> res = new ArrayList<String>();

        String query = "[{\"mid\":null," +
                "\"id\":\"/wikipedia/en_id/" + wikipedia_pageid + "\"" +
                "}]";

        GenericUrl url = new GenericUrl(properties.get(FB_QUERY_API_URL_MQL).toString());
        url.put("query", query);
        url.put("key", properties.get(FB_QUERY_API_KEY));

        HttpRequest request = requestFactory.buildGetRequest(url);
        HttpResponse httpResponse = interrupter.executeQuery(request, true);
        JSONObject response;
        try {
            response = (JSONObject) jsonParser.parse(httpResponse.parseAsString());
            JSONArray results = (JSONArray) response.get("result");

            for (Object result : results) {
                JSONObject obj = (JSONObject) result;
                String id = obj.get("mid").toString();
                res.add(id);
                //print or save this id
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        LOG.info("\tQueryFreebase:" + (new Date().getTime() - start.getTime()));

        return res;
    }

    //given a type search for any topics of that type and return their ids
    public List<String> mqlapi_topic_mids_with_name(String name, int maxResults) throws IOException {
        Date start = new Date();
        httpTransport = new NetHttpTransport();
        requestFactory = httpTransport.createRequestFactory();
        List<String> res = new ArrayList<String>();

        int limit = Integer.valueOf(properties.get(FB_QUERY_PARAM_LIMIT).toString());
        int iterations = maxResults % limit;
        iterations = iterations == 0 ? maxResults / limit : maxResults / limit + 1;
        String cursorPoint = "";
        for (int i = 0; i < iterations; i++) {
            String query = "[{\"mid\":null," +
                    "\"name\":\"" + name + "\"," +
                    "\"limit\":" + limit + "" +
                    "}]";

            GenericUrl url = new GenericUrl(properties.get(FB_QUERY_API_URL_MQL).toString());
            url.put("query", query);
            url.put("key", properties.get(FB_QUERY_API_KEY));
            url.put("cursor", cursorPoint);

            HttpRequest request = requestFactory.buildGetRequest(url);
            HttpResponse httpResponse = interrupter.executeQuery(request, true);
            System.out.println(limit * (i + 1));
            JSONObject response;
            try {
                response = (JSONObject) jsonParser.parse(httpResponse.parseAsString());
                cursorPoint = response.get("cursor").toString();
                JSONArray results = (JSONArray) response.get("result");

                for (Object result : results) {
                    JSONObject obj = (JSONObject) result;
                    String id = obj.get("mid").toString();
                    res.add(id);

                    //print or save this id
                }

                if (results.size() < limit) {
                    break;
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        LOG.info("\tQueryFreebase:" + (new Date().getTime() - start.getTime()));

        return res;
    }

    public List<String> mqlapi_instances_of_type(String name, int maxResults) throws IOException {
        Date start = new Date();
        httpTransport = new NetHttpTransport();
        requestFactory = httpTransport.createRequestFactory();
        List<String> res = new ArrayList<String>();

        int limit = Integer.valueOf(properties.get(FB_QUERY_PARAM_LIMIT).toString());
        int iterations = maxResults % limit;
        iterations = iterations == 0 ? maxResults / limit : maxResults / limit + 1;
        String cursorPoint = "";
        for (int i = 0; i < iterations; i++) {
            String query = "[{\"name\":null," +
                    "\"type\":\"" + name + "\"," +
                    "\"limit\":" + limit + "" +
                    "}]";

            GenericUrl url = new GenericUrl(properties.get(FB_QUERY_API_URL_MQL).toString());
            url.put("query", query);
            url.put("key", properties.get(FB_QUERY_API_KEY));
            url.put("cursor", cursorPoint);

            HttpRequest request = requestFactory.buildGetRequest(url);
            HttpResponse httpResponse = interrupter.executeQuery(request, true);
            System.out.println(limit * (i + 1));
            JSONObject response;
            try {
                response = (JSONObject) jsonParser.parse(httpResponse.parseAsString());
                cursorPoint = response.get("cursor").toString();
                JSONArray results = (JSONArray) response.get("result");

                for (Object result : results) {
                    JSONObject obj = (JSONObject) result;
                    String id = obj.get("name").toString();
                    res.add(id);

                    //print or save this id
                }

                if (results.size() < limit) {
                    break;
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        LOG.info("\tQueryFreebase:" + (new Date().getTime() - start.getTime()));

        return res;
    }

    /*public static void main(String[] args) throws IOException {
        FreebaseQueryHelper helper = new FreebaseQueryHelper("D:\\Work\\lodiedata\\tableminer_gs/freebase.properties");
        List<String> artist= helper.mqlapi_instances_of_type("/music/artist",10000);
        System.out.println(artist);
    }*/


    public double find_granularityForType(String type) throws IOException {
        if(type.startsWith("/m/")) //if the type id starts with "/m/" in strict sense it is a topic representing a concept
        //but is not listed as a type in freebase
            return 1.0;
        String url = properties.get(FB_HOMEPAGE).toString() +type+"?instances=";
        Date startTime = new Date();
        URL connection = new URL(url);
        BufferedReader in = new BufferedReader(
                new InputStreamReader(connection.openStream()));

        String result=null;
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            int start = inputLine.indexOf("span data-value=");
            if(start!=-1) {
                start+=16;
                int end = inputLine.indexOf(" ",16);
                if(start<end){
                    result = inputLine.substring(start,end).trim();
                    result = result.replaceAll("[^0-9]","");
                }
                break;
            }
        }
        in.close();
        LOG.info("\tFetchingFreebasePage:" + (new Date().getTime() - startTime.getTime()));
        if(result!=null && result.length()>0)
            return new Double(result);
        return 0.0;
    }
}
