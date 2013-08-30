package com.demo.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/******************************************
Created by: Borong Zhou
Created at: Aug 23, 2013:11:08:28 AM 
File: LineSegmentation.java
Comments:
 ******************************************/

public class LineSegmentation {

	private List<BNAPairPoints> segments = null;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		//
		Double[] variables = {3.0, 2.0, 1.0, 4.0, 12.0, 10.0,  5.0,  6.0, 9.0};
		Double[] values =    {1.0, 2.0, 3.0, 4.0,  5.0, 10.0, 12.0, 12.0, 10.0};
		LineSegmentation seg = new LineSegmentation();
		
		seg.setLineSegments(variables, values);
		
		System.out.println(seg.getSegments());
	}
	
	public void setLineSegments(List<BNAPoint> bnaPts) throws Exception {
		if (bnaPts == null
				|| bnaPts.size() < 2)
			throw new Exception("Input data is invalid!");
		
		segments = (segments == null)? new ArrayList<BNAPairPoints>() : segments;
		BNAPoint pt = null;
		
		Collections.sort(bnaPts);
		
		System.out.println(bnaPts);
		
		BNAPairPoints pair = null;
		BNAPoint startPt = bnaPts.get(0);
		BNAPoint endPt = null;
		for (int i = 1, n = bnaPts.size(); i < n; i++) {
			// TODO:: create line segments
			endPt = new BNAPoint(bnaPts.get(i).getValX(), bnaPts.get(i).getValY());
			if (bnaPts.get(i).getValY().equals(startPt.getValY())) {
				continue;
			}
			else {
				pair = new BNAPairPoints(1.0);
				pair.setStartPoint(startPt);
				pair.setEndPoint(endPt);
				segments.add(pair);
				
				startPt = new BNAPoint(endPt.getValX(), endPt.getValY());
			}
		}
	}
	
	public void setLineSegments(Double[] variables, Double[] values) throws Exception {
		if (variables == null
				|| values == null
				|| variables.length != values.length
				|| variables.length < 2)
			throw new Exception("Input data is invalid!");
		
		segments = (segments == null)? new ArrayList<BNAPairPoints>() : segments;
		List<BNAPoint> bnaPts = new ArrayList<BNAPoint>();
		BNAPoint pt = null;
		for (int i = 0, n = variables.length; i < n; i++) {
			pt = new BNAPoint(variables[i], values[i]);
			bnaPts.add(pt);
		}
		
		Collections.sort(bnaPts);
		
		this.setLineSegments(bnaPts);
	}
	
	public List<BNAPairPoints> getSegments() {
		return segments;
	}

	void validate(List<BNAPairPoints> segments) throws Exception {
		boolean invalid = false;
		
		if (segments != null && segments.size() > 1) {
			for (int i = 0, n = segments.size() - 1; i < n; i++) {
				if (segments.get(i).getStartPt().compareTo(segments.get(i+1).getStartPt()) == 1
						|| segments.get(i).getEndPt().compareTo(segments.get(i+1).getEndPt()) == 1) {
					
					invalid = true;
					break;
				}
			}
		}
		
		if (invalid)
			throw new Exception("Wrong line segment data!");
	}
	
	public List<BNAPoint> findPivotalPoints() {
		
		// adding endPoints to this list
		List<BNAPoint> points = new ArrayList<BNAPoint>();
		for (BNAPairPoints line : segments)
			points.add(line.getEndPt());
		
		return points;
	}
	
	// line segmentations are up or down
	public boolean isUp() {
		//
		
		return true;
	}
	
	boolean getMiddlePoints(List<BNAPairPoints> segments) {
		
		List<BNAPoint> recurList = new ArrayList<BNAPoint>();
		if (segments.size() == 1) {
			System.out.printf("is Up: %b\n", segments.get(0).isUp());
			
			return segments.get(0).isUp();
		}
		
		List<BNAPoint> pts = new ArrayList<BNAPoint>();
		for (BNAPairPoints pair : segments) {
			pts.add(pair.getMiddlePoint());
		}
		
		return getMiddlePoints(null);
	}
	
	public static class BNAPairPoints {
		private BNAPoint startPt = null;
		private BNAPoint endPt = null;
		private double scale = 0.5;
		
		public BNAPairPoints(double scale) {
			// used to test if the line segment is up
			if (scale <= 0.0)
				scale = 1.0;
			
			this.scale = scale;
		}
		
		public BNAPoint getStartPt() {
			return startPt;
		}
		public BNAPoint getEndPt() {
			return endPt;
		}
		public void setStartPoint(BNAPoint pt) {
			startPt = new BNAPoint(pt.getValX(), pt.getValY());
		}
		public void setEndPoint(BNAPoint pt) throws Exception {
			endPt = new BNAPoint(pt.getValX(), pt.getValY());
			if (isValid() == false)
				throw new Exception("Invalid End point!"); 
		}
		public BNAPoint getMiddlePoint() {
			
			return new BNAPoint((startPt.getValX() + endPt.getValX())/2, (startPt.getValY() + endPt.getValY())/2);
		}
		public boolean isUp() {
			return (endPt.getValY() - startPt.getValY()) > scale;
		}
		
		boolean isValid() {
			
			int retInt = this.getEndPt().compareTo(this.getStartPt());
			return  retInt > 0; 
		}
		
		@Override
		public int hashCode() {
			
			return this.getStartPt().hashCode() + 37 * this.getEndPt().hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			boolean isEqual = false;
			
			if (obj != null && (obj instanceof BNAPairPoints)) {
				BNAPairPoints other = (BNAPairPoints)obj;
				
				if (this.getStartPt().equals(other.getStartPt())
						&& this.getEndPt().equals(other.getEndPt()))
					
					isEqual = true;
			}
			
			return isEqual;
		}

		@Override
		public String toString() {
			
			return "{" + this.startPt.toString() + "," + this.getEndPt() + "}";
		}
	}

	@SuppressWarnings("rawtypes")
	public static class BNAPoint implements Comparable<BNAPoint> {
		Double valX;
		Double valY;
		boolean churn = false;
		
		public BNAPoint(Double x, Double y) {
			this.valX = x;
			this.valY = y;
		}
		public void setValues(BNAPoint pt) {
			this.valX = pt.valX;
			this.valY = pt.valY;
		}
		public Double getValX() {
			return valX;
		}
		public Double getValY() {
			return valY;
		}
		public boolean isChurn() {
			return churn;
		}
		public void setChurn(boolean churn) {
			this.churn = churn;
		}

		@Override
		public int hashCode() {
			
			return (int)Math.round(this.getValX() + 213 * this.getValY());
		}

		@Override
		public boolean equals(Object obj) {
			boolean isEqual = false;
			
			if ((obj != null) && (obj instanceof BNAPoint)) {
				BNAPoint other = (BNAPoint)obj;
				
				if (other.getValX() == this.getValX()
						&& other.getValY() == this.getValY())
					isEqual = true;
			}
			
			return isEqual;
		}
		
		@Override
		public int compareTo(BNAPoint obj) {

			// ascending
			return (int) Math.ceil(this.getValX() - obj.getValX());
		}
		
		@Override
		public String toString() {
			
			return "[" + this.valX + "," + this.valY + "]";
		}
	}
}
