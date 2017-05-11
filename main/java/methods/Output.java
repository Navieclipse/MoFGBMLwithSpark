package methods;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import gbml.Consts;

public class Output {

	Output(){}

	//******************************************************************************//

	//データファイル名作成
	public static void makeFileName(String dataName, String traFiles[][], String tstFiles[][], String hdfs, SparkSession sparkSession){

		for(int rep_i=0; rep_i<traFiles.length; rep_i++){
			for(int cv_i=0; cv_i<traFiles[0].length; cv_i++){
				 traFiles[rep_i][cv_i] = makeFileNameOne(dataName, hdfs, cv_i, rep_i, true, sparkSession);
				 tstFiles[rep_i][cv_i] = makeFileNameOne(dataName, hdfs, cv_i, rep_i, false, sparkSession);
			}
		}

	}

	public static String makeFileNameOne(String dataName, String hdfs, int cv_i, int rep_i, boolean isTra, SparkSession sparkSession){

		String fileName = "";
		if(isTra){
			if(sparkSession == null){
				fileName = hdfs + dataName + "/a" + Integer.toString(rep_i) + "_" + Integer.toString(cv_i) + "_" +dataName + "-10tra.dat";
			}else{
				fileName = hdfs + dataName + "/a" + Integer.toString(rep_i) + "_" + Integer.toString(cv_i) + "_" +dataName + "-10tra.csv";
			}
		}else{
			if(sparkSession == null){
				fileName = hdfs + dataName + "/a" + Integer.toString(rep_i) + "_" + Integer.toString(cv_i) + "_" +dataName + "-10tst.dat";
			}else{
				fileName = hdfs + dataName + "/a" + Integer.toString(rep_i) + "_" + Integer.toString(cv_i) + "_" +dataName + "-10tst.csv";
			}
		}
		return fileName;

	}

	//単一用
	public static void writeln(String fileName, String st, int os){

		try {
			//HDFSへの書き込み
			if(os == Consts.HDFS){
				Configuration conf = new Configuration();
				Path hdfsPath = new Path(fileName);
				FileSystem fs = FileSystem.get(conf);
				OutputStream stream = fs.create(hdfsPath, true);
			    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( stream, "UTF-8" ) );

			    br.append(st);
			    br.newLine();

			    br.close();
			    fs.close();
			}
			else{
				FileWriter fw = new FileWriter(fileName, true);
		        PrintWriter pw = new PrintWriter( new BufferedWriter(fw) );
		        pw.println(st);
		        pw.close();
			}
	    }
		catch (IOException ex){
			ex.printStackTrace();
	    }
	}

	//配列用
	public static void writeln(String fileName, String array[], int os){

		try {
			//HDFSへの書き込み
			if(os == Consts.HDFS){
				Configuration conf = new Configuration();
				Path hdfsPath = new Path(fileName);
				FileSystem fs = FileSystem.get(conf);
				OutputStream stream = fs.create(hdfsPath, true);
			    BufferedWriter br = new BufferedWriter( new OutputStreamWriter( stream, "UTF-8" ) );

			    for(int i=0; i<array.length; i++){
			    	br.append( array[i] );
			    	br.newLine();
			    }
			    br.close();
			    fs.close();
			}
			else{
				FileWriter fw = new FileWriter(fileName, true);
		        PrintWriter pw = new PrintWriter( new BufferedWriter(fw) );
		        for(int i=0; i<array.length; i++){
		        	 pw.println(array[i]);
			    }
		        pw.close();
			}
	    }
		catch (IOException ex){
			ex.printStackTrace();
	    }
	}

	//パラメータ出力用
	public static void writeSetting(String name ,String dir ,String st, int os){

		String sep = File.separator;
		String fileName = dir + sep + name + ".txt";

		writeln(fileName, st, os);
	}

	public static String makeDirName(String dataname, String hdfs, int executors, int exeCores, int seed, int os){

		String path = "";
		String sep = File.separator;
		//HDFS
		if(os != Consts.WINDOWS){
			path = System.getProperty("user.dir");
			path += sep + Consts.ROOTFOLDER +"_"+ dataname + "_e" + executors + "_c" + exeCores + "_" + seed;
		}
		else{
			path = hdfs + sep + Consts.ROOTFOLDER +"_"+ dataname + "_e" + executors + "_c" + exeCores + "_" + seed;
		}

		return path;
	}

	public static String makeDir(String dataname, String hdfs, int executors, int exeCores, int seed, int os){

		String path = "";
		String sep = File.separator;
		//HDFS
		if(os != Consts.HDFS){
			path = System.getProperty("user.dir");
			path += sep + Consts.ROOTFOLDER +"_"+ dataname + "_e" + executors + "_c" + exeCores + "_" + seed;
			File newdir = new File(path);
			newdir.mkdir();
		}
		else{
			path = hdfs + sep + Consts.ROOTFOLDER +"_"+ dataname + "_e" + executors + "_c" + exeCores + "_" + seed;
			Path hdfsPath = new Path( path );

			Configuration conf = new Configuration();
			try{
				FileSystem dfs = FileSystem.get(conf);
				dfs.mkdirs(hdfsPath);
			}
			catch (IOException ex){
				ex.printStackTrace();
		    }
		}

		return path;
	}

	public static void makeDirHDFS(String dirName){
		Path hdfsPath = new Path( dirName );
		Configuration conf = new Configuration();
		try{
			FileSystem dfs = FileSystem.get(conf);
			dfs.mkdirs(hdfsPath);
		}
		catch (IOException ex){
			ex.printStackTrace();
	    }
	}

	public static void makeDirRule(String dir, int os){

		String sep = File.separator;
		//HDFS
		if(os == Consts.HDFS){
			makeDirHDFS(dir + sep + Consts.RULESET);
			makeDirHDFS(dir + sep + Consts.VECSET);
			makeDirHDFS(dir + sep + Consts.SOLUTION);
			makeDirHDFS(dir + sep + Consts.OTHERS);
		}
		else{
			String path;
			path = dir + sep + Consts.RULESET;
			File newdir = new File(path);
			newdir.mkdir();

			path = dir + sep + Consts.VECSET;
			File newdir2 = new File(path);
			newdir2.mkdir();

			path = dir + sep + Consts.SOLUTION;
			File newdir3 = new File(path);
			newdir3.mkdir();

			path = dir + sep + Consts.OTHERS;
			File newdir4 = new File(path);
			newdir4.mkdir();
		}
	}

}
