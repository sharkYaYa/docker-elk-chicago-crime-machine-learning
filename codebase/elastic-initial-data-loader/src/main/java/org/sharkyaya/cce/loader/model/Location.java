package org.sharkyaya.cce.loader.model;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Location {
	
	/*
	 * "location":
		            {
		                "xCoordinate":"1189209",
		                "yCoordinate":"1857587",
		                "latitude":"41.76427307",
		                "longitude":"-87.58205731",
		                "location":"(41.764273074, -87.582057311)"
		            }
	 * */
	
	private long xCoordinate; /*X_COORDINATE*/
	private long yCoordinate; /*Y_COORDINATE*/
	private float latitude; /*LATITUDE*/
	private float longitude; /*LONGITUDE*/
	private String location; /*LOCATION*/
	
	@Override
	public String toString() {
		return ReflectionToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
	}

	public long getxCoordinate() {
		return xCoordinate;
	}

	public void setxCoordinate(long xCoordinate) {
		this.xCoordinate = xCoordinate;
	}

	public long getyCoordinate() {
		return yCoordinate;
	}

	public void setyCoordinate(long yCoordinate) {
		this.yCoordinate = yCoordinate;
	}

	public float getLatitude() {
		return latitude;
	}

	public void setLatitude(float latitude) {
		this.latitude = latitude;
	}

	public float getLongitude() {
		return longitude;
	}

	public void setLongitude(float longitude) {
		this.longitude = longitude;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}
	
	
	
}
