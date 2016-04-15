package com.uniplore;

public class Element {
	public int count=0;
	public String data=null;
	public void setCount(){
		this.count++;
	}
	public void setData(String s){
		data=s;
	}
	public String getData(){
		return data;
	}
	public int getCount(){
		return count;
	}

}
