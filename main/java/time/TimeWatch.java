package time;

public class TimeWatch {

	public TimeWatch(){}

	double start;
	double end;
	double time;
	double sectime;

	public void start(){
		start = System.nanoTime();
	}

	public void end(){
		end = System.nanoTime();
		time = end - start;
	}

	public void outnano(){
		System.out.println(time + "nanosec");
	}

	public double getSec(){
		return (time/1000000000.0);
	}

	public double getNano(){
		return time;
	}

	public double output(){
		sectime = (time/1000000000.0);
		return sectime;
	}

}
