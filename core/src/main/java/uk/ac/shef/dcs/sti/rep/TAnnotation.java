package uk.ac.shef.dcs.sti.rep;

import cern.colt.matrix.ObjectMatrix1D;
import cern.colt.matrix.ObjectMatrix2D;
import cern.colt.matrix.impl.SparseObjectMatrix1D;
import cern.colt.matrix.impl.SparseObjectMatrix2D;
import uk.ac.shef.dcs.sti.STIException;

import java.util.*;

/**
 */
public class TAnnotation {

    protected int rows;
    protected int cols;
    protected int subjectColumn;
    protected ObjectMatrix1D headerAnnotations; //each object in the matrix is an array of TColumnHeaderAnnotation
    protected ObjectMatrix2D contentAnnotations; //each object in the matrix is an array of TCellAnnotation
    protected Map<Key_SubjectCol_ObjectCol, Map<Integer, List<CellBinaryRelationAnnotation>>> relationAnnotations_per_row; //first key being the sub-obj column; second key is the row index
    private Map<Key_SubjectCol_ObjectCol, List<HeaderBinaryRelationAnnotation>> relationAnnotations_across_columns;

    public TAnnotation(int rows, int cols){
        this.rows=rows;
        this.cols=cols;
        headerAnnotations=new SparseObjectMatrix1D(cols);
        contentAnnotations = new SparseObjectMatrix2D(rows, cols);
        relationAnnotations_per_row =new HashMap<Key_SubjectCol_ObjectCol, Map<Integer, List<CellBinaryRelationAnnotation>>>();
        relationAnnotations_across_columns = new HashMap<Key_SubjectCol_ObjectCol, List<HeaderBinaryRelationAnnotation>>();
    }

    public int getRows(){
        return rows;
    }
    public int getCols(){
        return cols;
    }

    public void resetRelationAnnotations(){
        relationAnnotations_per_row.clear();
        relationAnnotations_across_columns.clear();
    }

    public void resetHeaderAnnotations(){
        headerAnnotations = new SparseObjectMatrix1D(headerAnnotations.size());
    }
    public void resetCellAnnotations(){
        contentAnnotations = new SparseObjectMatrix2D(contentAnnotations.rows(), contentAnnotations.columns());
    }

    /**
     * Target and Source must have the same dimension!!!
     * @param source
     * @param target
     * @return
     */
    public static void copy(TAnnotation source, TAnnotation target) throws STIException {
        if(source.getCols()!=target.getCols() || source.getRows()!=target.rows)
            throw new STIException("Source and target table annotation object has different dimensions!");

        for(int col=0; col<source.getCols(); col++){
            TColumnHeaderAnnotation[] annotations = source.getHeaderAnnotation(col);
            if(annotations==null)
                continue;
            TColumnHeaderAnnotation[] copy = new TColumnHeaderAnnotation[annotations.length];
            for(int index=0; index<annotations.length; index++){
                TColumnHeaderAnnotation ann = annotations[index];
                copy[index]= TColumnHeaderAnnotation.copy(ann);
            }
            target.setHeaderAnnotation(col, copy);
        }

        for(int row=0; row<source.getRows();row++){
            for(int col=0; col<source.getCols(); col++){
                TCellAnnotation[] annotations = source.getContentCellAnnotations(row, col);
                if(annotations==null)
                    continue;
                TCellAnnotation[] copy = new TCellAnnotation[annotations.length];
                for(int index=0; index<annotations.length;index++)
                    copy[index]= TCellAnnotation.copy(annotations[index]);
                target.setContentCellAnnotations(row, col,copy);
            }
        }
        target.relationAnnotations_per_row = new HashMap<>(
                source.getRelationAnnotations_per_row()
        );
        target.relationAnnotations_across_columns = new HashMap<>(
                source.getRelationAnnotations_across_columns()
        );
    }

    public void setHeaderAnnotation(int headerCol, TColumnHeaderAnnotation[] annotations){
        Set<TColumnHeaderAnnotation> deduplicateCheck = new HashSet<TColumnHeaderAnnotation>(Arrays.asList(annotations));
        if(deduplicateCheck.size()!=annotations.length)
            System.err.println("duplicate header anntoations "+headerCol+":"+deduplicateCheck);

        headerAnnotations.set(headerCol, annotations);
    }
    public TColumnHeaderAnnotation[] getHeaderAnnotation(int headerCol){
        Object o=headerAnnotations.get(headerCol);
        if(o==null)
            return new TColumnHeaderAnnotation[0];
        TColumnHeaderAnnotation[] ha = (TColumnHeaderAnnotation[]) o;
        Arrays.sort(ha);

        return ha;
    }

    public List<TColumnHeaderAnnotation> getWinningHeaderAnnotations(int headerCol){
        TColumnHeaderAnnotation[] annotations =getHeaderAnnotation(headerCol);

        List<TColumnHeaderAnnotation> result = new ArrayList<TColumnHeaderAnnotation>();
        if(annotations==null||annotations.length==0)
            return result;
        double prevScore = 0.0;
        for(TColumnHeaderAnnotation h: annotations){
            if(prevScore==0.0){
                prevScore=h.getFinalScore();
                result.add(h);
                continue;
            }
            if(h.getFinalScore()==prevScore)
                result.add(h);
            else
                break;
        }
        return result;
    }

    public void setContentCellAnnotations(int row, int col, TCellAnnotation[] annotations){
        Set<TCellAnnotation> deduplicateCheck = new HashSet<TCellAnnotation>(Arrays.asList(annotations));
        if(deduplicateCheck.size()!=annotations.length)
            System.err.println("duplicate cell anntoations "+row+","+col+":"+deduplicateCheck);
        contentAnnotations.set(row, col, annotations);
    }
    public TCellAnnotation[] getContentCellAnnotations(int row, int col){
        Object o = contentAnnotations.get(row, col);
        if(o==null)
            return new TCellAnnotation[0];
        TCellAnnotation[] ca = (TCellAnnotation[]) o;
        Arrays.sort(ca);
        return ca;
    }

    public List<TCellAnnotation> getWinningContentCellAnnotation(int row, int col){
        TCellAnnotation[] annotations =getContentCellAnnotations(row, col);

        List<TCellAnnotation> result = new ArrayList<TCellAnnotation>();
        if(annotations==null||annotations.length==0)
            return result;
        double prevScore = 0.0;
        for(TCellAnnotation c: annotations){
            if(prevScore==0.0){
                prevScore=c.getFinalScore();
                result.add(c);
                continue;
            }
            if(c.getFinalScore()==prevScore)
                result.add(c);
            else
                break;
        }
        return result;
    }

    public void addRelationAnnotation_per_row(CellBinaryRelationAnnotation ra){

        Map<Integer, List<CellBinaryRelationAnnotation>> annotations_for_columns
                = relationAnnotations_per_row.get(ra.getSubject_object_key());          //get the container of binary relations between the two columns
        if(annotations_for_columns==null)
            annotations_for_columns=new HashMap<Integer, List<CellBinaryRelationAnnotation>>();
        List<CellBinaryRelationAnnotation> annotations_for_row=annotations_for_columns.get(ra.getRow()); //get the container for binary relations for that row, and between the two columns
        if(annotations_for_row==null)
            annotations_for_row = new ArrayList<CellBinaryRelationAnnotation>();

        if(annotations_for_row.contains(ra)){
            //System.out.println(ra);
            /*CellBinaryRelationAnnotation contained_ra =annotations_for_row.get(annotations_for_row.indexOf(ra));
            contained_ra.setScore(contained_ra.getScore()+ra.getScore());
            contained_ra.addMatched_values(ra.getMatched_values());*/
            double new_score =ra.getScore();
            CellBinaryRelationAnnotation contained_ra =annotations_for_row.get(annotations_for_row.indexOf(ra));
            if(contained_ra.getScore()<new_score)
                contained_ra.setScore(new_score);
            contained_ra.addMatched_values(ra.getMatched_values());
        }   else{
        annotations_for_row.add(ra);  //container for that row
        }
        annotations_for_columns.put(ra.getRow(), annotations_for_row); //container for that column
        relationAnnotations_per_row.put(ra.getSubject_object_key(), annotations_for_columns);
    }
    public Map<Integer, List<CellBinaryRelationAnnotation>> getRelationAnnotationsBetween(int subjectCol, int objectCol){
        Key_SubjectCol_ObjectCol binary_key = new Key_SubjectCol_ObjectCol(subjectCol, objectCol);
        return relationAnnotations_per_row.get(binary_key);
    }

    public Map<Key_SubjectCol_ObjectCol, Map<Integer, List<CellBinaryRelationAnnotation>>> getRelationAnnotations_per_row(){
        return relationAnnotations_per_row;
    }

    public int getSubjectColumn() {
        return subjectColumn;
    }

    public void setSubjectColumn(int subjectColumn) {
        this.subjectColumn = subjectColumn;
    }

    public Map<Key_SubjectCol_ObjectCol, List<HeaderBinaryRelationAnnotation>> getRelationAnnotations_across_columns() {
        return relationAnnotations_across_columns;
    }

    public void addRelationAnnotation_across_column(HeaderBinaryRelationAnnotation ra){
        if(ra.getAnnotation_url().startsWith("/type"))
            System.out.println();
        List<HeaderBinaryRelationAnnotation> annotations_for_columns
                = relationAnnotations_across_columns.get(ra.getSubject_object_key());          //get the container of binary relations between the two columns
        if(annotations_for_columns==null)
            annotations_for_columns=new ArrayList<HeaderBinaryRelationAnnotation>();
        if(annotations_for_columns.contains(ra))
            System.err.println("hbr already contained");
        annotations_for_columns.add(ra);
        relationAnnotations_across_columns.put(ra.getSubject_object_key(), annotations_for_columns);
    }

    public List<HeaderBinaryRelationAnnotation> getBestRelationAnnotationsBetween(int subjectCol, int objectCol){
        Key_SubjectCol_ObjectCol binary_key = new Key_SubjectCol_ObjectCol(subjectCol, objectCol);
        return getBestRelationAnnotationsBetween(binary_key);
    }

    public List<HeaderBinaryRelationAnnotation> getBestRelationAnnotationsBetween(Key_SubjectCol_ObjectCol subobj){
        List<HeaderBinaryRelationAnnotation> candidates = relationAnnotations_across_columns.get(subobj);
        Collections.sort(candidates);

        List<HeaderBinaryRelationAnnotation> result = new ArrayList<HeaderBinaryRelationAnnotation>();
        double maxScore = candidates.get(0).getFinalScore();
        for(HeaderBinaryRelationAnnotation hbr: candidates){
            if(hbr.getFinalScore()==maxScore)
                result.add(hbr);
        }
        return result;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }
}
