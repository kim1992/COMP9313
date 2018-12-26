package comp9313.proj1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class StringPair implements WritableComparable<StringPair> {

	private String first;
	private String second;

	public StringPair() {
	}

	public StringPair(String first, String second) {
		set(first, second);
	}

	public void set(String left, String right) {
		first = left;
		second = right;
	}

	public String getFirst() {
		return first;
	}

	public String getSecond() {
		return second;
	}

	public void readFields(DataInput in) throws IOException {
		String[] strings = WritableUtils.readStringArray(in);
		first = strings[0];
		second = strings[1];
	}

	public void write(DataOutput out) throws IOException {
		String[] strings = new String[] { first, second };
		WritableUtils.writeStringArray(out, strings);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append(first + " " + second);
		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		StringPair that = (StringPair) o;

		if (first != null ? !first.equals(that.first) : that.first != null)
			return false;
		if (second != null ? !second.equals(that.second) : that.second != null)
			return false;
		
		return true;
	}

	@Override
	public int hashCode() {
		int result = first != null ? first.hashCode() : 0;
		result = 31 * result + (second != null ? second.hashCode() : 0);
		return result;
		//return first.hashCode();
	}
	
	public static boolean isNumeric(String str){
		Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
		return pattern.matcher(str).matches();
	}
	
	private int compareFirst(String s1, String s2){
		if (s1 == null && s2 != null) {
			return -1;
		} else if (s1 != null && s2 == null) {
			return 1;
		} else if (s1 == null && s2 == null) {
			return 0; 
		} else {
			return s1.compareTo(s2);
		}
	}
	
	public int compareSecond(String s1, String s2){
		if (isNumeric(s1) && isNumeric(s2)){
			int ints1 = Integer.parseInt(s1);
			int ints2 = Integer.parseInt(s2);
			if (ints1 != ints2){
				return (ints1 < ints2? -1: 1);
			} else {
				return 0;
			}
			
		} else {
			return s1.compareTo(s2);
		}
		
	}

	@Override
	public int compareTo(StringPair o) {
		int cmp = compareFirst(first, o.getFirst());
		if(cmp != 0){
			return cmp;
		}
		return compareSecond(second, o.getSecond());
	}	


}
