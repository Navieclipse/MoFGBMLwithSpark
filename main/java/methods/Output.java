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

import navier.Cons;

public class Output {

	Output(){}

	//******************************************************************************//

	//データファイル名作成
	public static void makeFileName(String dataName, String traFiles[][], String tstFiles[][], String hdfs){

		for(int rep_i=0; rep_i<traFiles.length; rep_i++){
			for(int cv_i=0; cv_i<traFiles[0].length; cv_i++){
				 traFiles[rep_i][cv_i] = hdfs + dataName + "/a" + Integer.toString(rep_i) + "_" + Integer.toString(cv_i) + "_" +dataName + "-10tra.csv";
				 tstFiles[rep_i][cv_i] = hdfs + dataName + "/a" + Integer.toString(rep_i) + "_" + Integer.toString(cv_i) + "_" +dataName + "-10tst.csv";
			}
		}

	}

	//単一用
	public static void writeln(String fileName, String st, int os){

		try {
			//HDFSへの書き込み
			if(os == Cons.HDFS){
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
			if(os == Cons.HDFS){
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
	public static void writeExp(String name ,String dir ,String st, int os){

		String fileName;
		if(os == Cons.Win){
			fileName = dir + "\\" + name + ".txt";
		}else{
			fileName = dir + "/" + name + ".txt";
		}

		writeln(fileName, st, os);
	}

	public static String makeDir(String dataname, String hdfs, int executors, int exeCores, int seed, int os){

		String path = "";
		//HDFS
		if(os == Cons.Win){
			path = System.getProperty("user.dir");
			path += "\\result_" + dataname + "_e" + executors + "_c" + exeCores + "_" + seed;
			File newdir = new File(path);
			newdir.mkdir();
		}
		else{
			path = hdfs +  "/result_" + dataname + "_e" + executors + "_c" + exeCores + "_" + seed;
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

		//HDFS
		if(os == Cons.Uni){
			makeDirHDFS(dir + "/ruleset");
			makeDirHDFS(dir + "/vecset");
			makeDirHDFS(dir + "/solution");
			makeDirHDFS(dir + "/write");

		}
		else{
			String path;
			path = dir + "\\ruleset";
			File newdir = new File(path);
			newdir.mkdir();

			path = dir + "\\vecset";
			File newdir2 = new File(path);
			newdir2.mkdir();

			path = dir + "\\solution";
			File newdir3 = new File(path);
			newdir3.mkdir();

			path = dir + "\\write";
			File newdir4 = new File(path);
			newdir4.mkdir();
		}
	}

}
