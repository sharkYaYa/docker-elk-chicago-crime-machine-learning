package org.sharkyaya.cce.loader.model;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Address {

	/*
	 * "address":
		    {
		        "communityArea":"43",
		        "block":"017XX E 72ND ST",
		        "ward":"5",
		        "district":"3",
		     location...   
		       
		    }
	 * */
	
	private String communityArea; /*COMMUNITY_AREA*/
	private String block; /*BLOCK*/
	private String ward; /*WARD*/
	private String district; /*DISTRICT*/
	
	private Location location;

	@Override
	public String toString() {
		return ReflectionToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
	}
	
	
	
	public String getCommunityArea() {
		return communityArea;
	}

	public void setCommunityArea(String communityArea) {
		this.communityArea = communityArea;
	}

	public String getBlock() {
		return block;
	}

	public void setBlock(String block) {
		this.block = block;
	}

	public String getWard() {
		return ward;
	}

	public void setWard(String ward) {
		this.ward = ward;
	}

	public String getDistrict() {
		return district;
	}

	public void setDistrict(String district) {
		this.district = district;
	}

	public Location getLocation() {
		return location;
	}

	public void setLocation(Location location) {
		this.location = location;
	}
	
}
