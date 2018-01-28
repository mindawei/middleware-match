package com.alibaba.middleware.race.jstorm;

import java.io.Serializable;

public class ValidInfo implements Serializable{

	private static final long serialVersionUID = 23547788L;
	
	private double money;
	private long time;
	private short kind;
	
	public ValidInfo() {
		
	}

	public ValidInfo(double money, long time, short kind) {
		super();
		this.time = time;
		this.money = money;
		this.kind = kind;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public double getMoney() {
		return money;
	}

	public void setMoney(double money) {
		this.money = money;
	}

	public short getKind() {
		return kind;
	}

	public void setKind(short kind) {
		this.kind = kind;
	}
	
}
