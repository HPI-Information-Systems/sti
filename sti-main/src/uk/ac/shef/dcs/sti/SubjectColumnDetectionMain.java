package uk.ac.shef.dcs.sti;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.json.JSONException;
import uk.ac.shef.dcs.sti.core.algorithm.tmp.sampler.TContentTContentRowRankerImpl;
import uk.ac.shef.dcs.sti.core.model.*;
import uk.ac.shef.dcs.sti.core.subjectcol.SubjectColumnDetector;
import uk.ac.shef.dcs.sti.util.FileUtils;
import uk.ac.shef.dcs.websearch.WebSearchException;
import uk.ac.shef.dcs.websearch.bing.v2.APIKeysDepletedException;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;

public class SubjectColumnDetectionMain {

    protected static final String PROPERTY_HOME = "sti.home";
    protected static final String PROPERTY_NLP_RESOURCES = "sti.nlp";
    protected static final String PROPERTY_TMP_IINF_WEBSEARCH_STOPPING_CLASS = "sti.iinf.websearch.stopping.class";
    protected static final String PROPERTY_TMP_IINF_WEBSEARCH_STOPPING_CLASS_CONSTR_PARAM
            = "sti.iinf.websearch.stopping.class.constructor.params";
    protected static final String PROPERTY_CACHE_FOLDER = "sti.cache.main.dir";
    private static final String PROPERTY_WEBSEARCH_CACHE_CORENAME = "websearch";
    protected static final String PROPERTY_WEBSEARCH_PROP_FILE = "sti.websearch.properties";
    private CustomSubjectColumnDetector detector;


    public static void main(String[] args) throws STIException, IOException, WebSearchException, APIKeysDepletedException, ClassNotFoundException, JSONException {
        SubjectColumnDetectionMain detector = new SubjectColumnDetectionMain();
        detector.runExperiment(args[0]);
    }

    private void runExperiment(String inputPath) throws IOException, APIKeysDepletedException, ClassNotFoundException {
        JsonParser parser = new JsonParser();
        JsonElement obj = parser.parse(new FileReader(new File(inputPath)));
        JsonArray arr = obj.getAsJsonObject().getAsJsonArray("tables");
        Map<Table,Pair<HashSet<Integer>,HashSet<Integer>>> tables = new HashMap<>();
        for (int i = 0; i < arr.size(); i++)
        {
            JsonObject curobj = arr.get(i).getAsJsonObject();
            String filename = curobj.get("sequenceFile").getAsString();
            HashSet<Integer> localKeys = getSingleColumnKeys(curobj.get("localKeys"));
            HashSet<Integer> globalKeys = getSingleColumnKeys(curobj.get("globalKeys"));
            int tableID = curobj.get("tableID").getAsInt();
            JsonArray content = curobj.get("content").getAsJsonArray();
            Table table = parseTable(filename + "," + tableID,content);
            String title = getFieldNullSafe(curobj, "title");
            String caption = getFieldNullSafe(curobj, "caption");
            String paraBefore = getFieldNullSafe(curobj, "paraBefore");
            String paraAfter = getFieldNullSafe(curobj, "paraAfter");
            table.addContext(new TContext(title, TContext.TableContextType.PAGETITLE,1.0));
            table.addContext(new TContext(caption, TContext.TableContextType.CAPTION,1.0));
            table.addContext(new TContext(paraBefore, TContext.TableContextType.PARAGRAPH_BEFORE,1.0));
            table.addContext(new TContext(paraAfter, TContext.TableContextType.PARAGRAPH_AFTER,1.0));
            tables.put(table, new ImmutablePair<>(localKeys,globalKeys));
        }
        System.out.println(tables.size());
        detector.setKeyColumns(tables);
        double maxLocal =-1.0;
        double maxGlobal = -1.0;
        int maxLocalI = -1;
        int maxGlobalI = -1;
        System.out.println("keytype,experiment,threshold,precision,recall");
        ConfusionMatrix prevLocal =null;
        ConfusionMatrix prevGlobal = null;
        for (int i = 0; i < 100; i++) {
            double curThreshold = 0.01*(i+1);
            ConfusionMatrix localMatrix = new ConfusionMatrix();
            ConfusionMatrix globalMatrix = new ConfusionMatrix();
            getResultsForThreshold(false, tables, curThreshold, localMatrix);
            getResultsForThreshold(true, tables, curThreshold, globalMatrix);
            double globalAccuracy = globalMatrix.f1();
            double localAccuracy = localMatrix.f1();
            if(prevGlobal==null || globalMatrix.precision()!=prevGlobal.precision() || globalMatrix.recall()!=prevGlobal.recall())
                System.out.println("global,ZH," + curThreshold + "," + globalMatrix.precision() + "," + globalMatrix.recall());
            if(prevLocal==null || localMatrix.precision()!=prevLocal.precision() || localMatrix.recall()!=prevLocal.recall())
                System.out.println("local,ZH," + curThreshold + "," + localMatrix.precision() + "," + localMatrix.recall());
            prevGlobal = globalMatrix;
            prevLocal = localMatrix;
            if(globalAccuracy > maxGlobal){
                maxGlobal = globalAccuracy;maxGlobalI = i;
            }
            if(localAccuracy > maxLocal){
                maxLocal = localAccuracy;maxLocalI= i;
            }
        }
        ConfusionMatrix localMatrix = new ConfusionMatrix();
        ConfusionMatrix globalMatrix = new ConfusionMatrix();
        double localThreshold = 0.01*(maxLocalI+1);
        double globalThreshold = 0.01*(maxGlobalI+1);
        getResultsForThreshold(false, tables, localThreshold, localMatrix);
        getResultsForThreshold(true, tables, globalThreshold, globalMatrix);
        double globalAccuracy = globalMatrix.f1();
        double localAccuracy = localMatrix.f1();
        System.out.println("" + globalThreshold + "," + globalAccuracy);
        System.out.println("" + localThreshold + "," + localAccuracy);
    }

    private void getResultsForThreshold(boolean global, Map<Table, Pair<HashSet<Integer>, HashSet<Integer>>> tables, double curThreshold, ConfusionMatrix matrix) throws APIKeysDepletedException, IOException, ClassNotFoundException {
        for (Table table : tables.keySet()) {
            Pair<HashSet<Integer>, HashSet<Integer>> keys = tables.get(table);
            HashSet<Integer> localKeys = keys.getKey();
            HashSet<Integer> globalKeys = keys.getValue();
            String id = table.getTableId();
            List<Pair<Integer, Pair<Double, Boolean>>> res = computeSubjectColumn(table);
            if(global)
                processResults(globalKeys,matrix,res, curThreshold);
            else
                processResults(localKeys,matrix,res, curThreshold);
        }
    }

    private static String getFieldNullSafe(JsonObject curobj, String key) {
        if(curobj.get(key).isJsonNull()) return "";
        else return curobj.get(key).getAsString();
    }

    private static void processResults(HashSet<Integer> localKeys, ConfusionMatrix localMatrix, List<Pair<Integer, Pair<Double, Boolean>>> predictedKey, double threshold) {
        boolean noPrediction = predictedKey.isEmpty() || predictedKey.get(0).getValue().getKey() < threshold;
        if(noPrediction && localKeys.isEmpty()){
            localMatrix.tn++;
        } else if(noPrediction && !localKeys.isEmpty())
            localMatrix.fn++;
        else if(localKeys.contains(predictedKey.get(0).getKey()))
            localMatrix.tp++;
        else
            localMatrix.fp++;
    }

    private static HashSet<Integer> getSingleColumnKeys(JsonElement localKeys) {
        JsonArray array = localKeys.getAsJsonArray();
        HashSet<Integer> keys = new HashSet<>();
        for (int i = 0; i < array.size(); i++) {
            JsonArray curKey = array.get(i).getAsJsonArray();
            if(curKey.size()==1)
                keys.add(curKey.get(0).getAsInt());
        }
        return keys;
    }

    private static Table parseTable(String id,JsonArray content) {
        int nrows = content.size();
        if(content.size()==0){
            System.out.println("what?");
            Table table = new Table(id,"",1,1);
            table.setColumnHeader(0,new TColumnHeader("0"));
            table.setContentCell(0,0,new TCell(""));
            return table;
        }
        int ncols = content.get(0).getAsJsonArray().size();
        Table table = new Table(id,"",nrows,ncols);
        for (int i = 0; i < content.size(); i++){
            JsonArray curRow = content.get(i).getAsJsonArray();
            for (int j = 0; j < curRow.size(); j++){
                String value = curRow.get(j).getAsString();
                table.setColumnHeader(j,new TColumnHeader(""+j));
                table.setContentCell(i,j,new TCell(value));
            }
        }
        return table;
    }

    public SubjectColumnDetectionMain() throws WebSearchException, STIException, IOException, APIKeysDepletedException, ClassNotFoundException {
        init();
    }

    private void init() throws IOException, STIException, WebSearchException, APIKeysDepletedException, ClassNotFoundException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(new File("config/sti.properties")));
        String solrHomePath = properties.getProperty(PROPERTY_CACHE_FOLDER);
        EmbeddedSolrServer embeddedSolrServer = new EmbeddedSolrServer(Paths.get(solrHomePath), PROPERTY_WEBSEARCH_CACHE_CORENAME);
        detector = new CustomSubjectColumnDetector(new TContentTContentRowRankerImpl(),
                properties.getProperty(PROPERTY_TMP_IINF_WEBSEARCH_STOPPING_CLASS),
                StringUtils.split(properties.getProperty(PROPERTY_TMP_IINF_WEBSEARCH_STOPPING_CLASS_CONSTR_PARAM)),
                embeddedSolrServer,
                getNLPResourcesDir(properties),
                false, //Boolean.valueOf(properties.getProperty(PROPERTY_TMP_SUBJECT_COLUMN_DETECTION_USE_WEBSEARCH)),
                getStopwords(properties),
                properties.getProperty(PROPERTY_HOME)
                        + File.separator + properties.getProperty(PROPERTY_WEBSEARCH_PROP_FILE)
        );
    }

    public List<Pair<Integer, Pair<Double, Boolean>>> computeSubjectColumn(Table table) throws APIKeysDepletedException, IOException, ClassNotFoundException {
        return detector.compute(table);
    }

    static List<String> getStopwords(Properties properties) throws STIException, IOException {
        return FileUtils.readList(getNLPResourcesDir(properties) + File.separator + "stoplist.txt", true);
    }

    static String getNLPResourcesDir(Properties properties) throws STIException {
        String prop = properties.getProperty(PROPERTY_HOME)
                + File.separator + properties.getProperty(PROPERTY_NLP_RESOURCES);
        if (prop == null || !new File(prop).exists()) {
            throw new STIException("Cannot proceed: nlp resources folder is not set or does not exist.");
        }
        return prop;
    }

    private static TCell getRandomContent() {
        return new TCell("content");
    }
}
