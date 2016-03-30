package uk.ac.shef.dcs.kbsearch.freebase;

import uk.ac.shef.dcs.kbsearch.KBSearchResultFilter;
import uk.ac.shef.dcs.kbsearch.rep.Attribute;
import uk.ac.shef.dcs.kbsearch.rep.Clazz;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: zqz
 * Date: 16/02/14
 * Time: 14:25
 * To change this template use File | Settings | File Templates.
 */
public class FreebaseSearchResultFilter extends KBSearchResultFilter {

    //todo: what is this?
    private static final boolean IGNORE_NOTABLE_EXTRACTED_TYPE=false;


    //todo MOVE THESE "stop" LIST TO FILES
    private static List<String> FACTS_TO_IGNORE_BY_PREDICATES = new ArrayList<>();

    static {
        FACTS_TO_IGNORE_BY_PREDICATES = Arrays.asList(
                new String[]{
                        "/type/permission/controls",
                        "/media_common/creative_work/credit",
                        "/type/object/timestamp",
                        "/type/object/creator",
                        "creator",
                        "lang",
                        "timestamp",
                        "count",
                        "/type/object/key",
                        "/type/object/attribution",
                        "/type/object/permission",
                        "/type/object/guid",
                        "/common/document/updated"
                }
        );
    }

    private static boolean ignoreType(String type, String label) {
        if (type.startsWith("/user/") ||
                type.startsWith("/common/")||
                //type.equals("/common/image") ||
                /*type.equals("/type/content") ||
                type.startsWith("/type/type/domain")||*/
                type.startsWith("/type/")||
                type.endsWith("topic") || type.startsWith("/pipeline/") ||
                type.endsWith("skos_concept") ||
                type.endsWith("_instance") ||
                type.startsWith("/base/type_ontology")
                ||label.equalsIgnoreCase("topic")||label.equalsIgnoreCase("thing")||label.equalsIgnoreCase("concept")
                ||label.equalsIgnoreCase("things")||label.equalsIgnoreCase("entity"))
            return true;

        if (IGNORE_NOTABLE_EXTRACTED_TYPE &&
                type.startsWith("/m/"))
            return true;

        return false;

    }

    public static List<Clazz> filterTypes(List<Clazz> types){
        List<Clazz> r = new ArrayList<>();
        for(Clazz t: types) {
            String url = t.getId();
            String label = t.getLabel();
            if (ignoreType(url, label)) continue;
            r.add(t);
        }
        return r;
    }

    private static boolean ignoreRelation(String relation) {
        if (/*relation.equals("/common/topic/article") ||
                relation.equals("/common/topic/image") ||
                relation.equals("/common/document/text") ||
                relation.equals("/common/topic/description") ||
                relation.equals("/common/topic/notable_properties")||
                relation.equals("/common/document/updated")||
                relation.equals("/common/document/content")||
                relation.equals("/common/document/text")||*/
                relation.startsWith("/common/")||
                //relation.equals("/common/document/source_uri") ||
                relation.equals("id") ||
                /*relation.equals("/type/object/type") ||
                relation.equals("/type/object/name") ||
                relation.equals("/type/object/mid") ||
                relation.equals("/type/object/id")||
                relation.equals("/type/object/attribution")||
                relation.equals("/type/object/permission")||
                relation.equals("/type/object/key")||
                relation.equals("/type/type/domain")||
                relation.equals("/type/type/properties")||
                relation.equals("/type/type/expected_by")*/
                relation.startsWith("/type/")
                )

            return true;
        return false;
    }

    public static List<Attribute> filterRelations(List<Attribute> facts){
        List<Attribute> r = new ArrayList<>();
        for(Attribute t: facts) {
            String url = t.getRelation();
            if (ignoreRelation(url)) continue;
            r.add(t);
        }
        return r;
    }

    @Deprecated
    public static boolean ignoreFactFromBOW(String predicate) {
        return false; //predicate should have been fitltered by calling ignorePreidcate_from_triple
    }

    protected static boolean ignoreFactByPredicate(String s) {
        if (FACTS_TO_IGNORE_BY_PREDICATES.contains(s))
            return true;
        return false;
    }
}
