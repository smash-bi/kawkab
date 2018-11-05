package kawkab.fs.utils;

public class Logger {
	private enum LogLevel {DISABLED, WARN, INFO, DEBUG}
	private static Logger log;
	private LogLevel logLevel = LogLevel.INFO;
	
	public synchronized static Logger get(){
		if (log == null){
			log = new Logger();
		}
		
		return log;
	}
	
	public synchronized void warn(String msg){
		if (logLevel.ordinal() >= LogLevel.WARN.ordinal()){
			System.out.println(msg);
		}
	}
	
	public synchronized void info(String msg){
		if (logLevel.ordinal() >= LogLevel.INFO.ordinal()){
			System.out.println(msg);
		}
	}
	
	public synchronized void debug(String msg){
		if (logLevel.ordinal() >= LogLevel.DEBUG.ordinal()){
			System.out.println(msg);
		}
	} 
}
