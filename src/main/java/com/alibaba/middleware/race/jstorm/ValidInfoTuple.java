package com.alibaba.middleware.race.jstorm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ValidInfoTuple implements Serializable{

	private static final long serialVersionUID = 669978L;
	
	private List<ValidInfo> infos = new ArrayList<>();

	public ValidInfoTuple() {
	}

	public List<ValidInfo> getValidInfos() {
		return infos;
	}
	
	public void addValidInfos(List<ValidInfo> addInfos) {
		infos.addAll(addInfos);
	}

}
