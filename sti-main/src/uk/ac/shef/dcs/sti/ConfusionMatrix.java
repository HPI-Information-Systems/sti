package uk.ac.shef.dcs.sti;

public class ConfusionMatrix {
    public int tp=0,fp=0,tn=0,fn=0;

    public double accuracy() {
        return (tp + tn) / ((double) (tp+tn+fp+fn));
    }

    public double f1() {
        return (2*tp) / ((double) 2*tp+fp+fn);
    }

    public double precision() {
        return (tp) / ((double) tp+fp);
    }

    public double recall() {
        return (tp) / ((double) tp+fn);
    }
}
