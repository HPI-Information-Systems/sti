package uk.ac.shef.dcs.sti;

import cern.colt.matrix.DoubleMatrix2D;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import uk.ac.shef.dcs.sti.core.algorithm.tmp.sampler.TContentRowRanker;
import uk.ac.shef.dcs.sti.core.algorithm.tmp.stopping.StoppingCriteriaInstantiator;
import uk.ac.shef.dcs.sti.core.model.Table;
import uk.ac.shef.dcs.sti.core.subjectcol.SubjectColumnDetector;
import uk.ac.shef.dcs.sti.core.subjectcol.TColumnFeature;
import uk.ac.shef.dcs.websearch.WebSearchException;
import uk.ac.shef.dcs.websearch.bing.v2.APIKeysDepletedException;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CustomSubjectColumnDetector extends SubjectColumnDetector {

    public Map<Table,Pair<HashSet<Integer>,HashSet<Integer>>> getKeyColumns() {
        return keyColumns;
    }

    public void setKeyColumns(Map<Table,Pair<HashSet<Integer>,HashSet<Integer>>> keyColumns) {
        this.keyColumns = keyColumns;
    }

    private Map<Table, Pair<HashSet<Integer>,HashSet<Integer>>> keyColumns;

    public CustomSubjectColumnDetector(TContentRowRanker tRowRanker, String stoppingCriteriaClassname, String[] stoppingCriteriaParams, EmbeddedSolrServer cache, String nlpResource, boolean useWS, List<String> stopwords, String webSearchPropFile) throws IOException, WebSearchException {
        super(tRowRanker, stoppingCriteriaClassname, stoppingCriteriaParams, cache, nlpResource, useWS, stopwords, webSearchPropFile);
    }

    @Override
    protected void computeWSScores(Table table, List<TColumnFeature> featuresOfNEColumns) throws APIKeysDepletedException, IOException, ClassNotFoundException {
        for (TColumnFeature cf : featuresOfNEColumns) {
            if(keyColumns.get(table).getKey().contains(cf.getColId()) || keyColumns.get(table).getValue().contains(cf.getColId())){
                cf.setWebSearchScore(1.0);
            } else{
                cf.setWebSearchScore(0.0);
            }
        }
    }
}
