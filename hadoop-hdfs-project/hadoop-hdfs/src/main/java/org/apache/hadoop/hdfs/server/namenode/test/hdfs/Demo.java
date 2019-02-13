package org.apache.hadoop.hdfs.server.namenode.test.hdfs;

public class Demo
{

    public  static void test(int []a){
        int num=0;
        for(int i = 1 ; i <= 7; i ++){

            for(int j =  1 ; j <= 7 ; j ++){
                for(int k =  1 ; k <= 7 ; k ++) {

                 /*   if (k == i||k==j||i==j) {
                        continue;
                    }*/
                    num++;
                    System.out.println(i + "" + j+""+k);
                }
            }

        }
        System.out.println("num===="+num);
    }
    public static void test1(){

    }
    public static void main(String[] args) {
                       
        System.out.println(Math.sqrt(8));
    }
}
