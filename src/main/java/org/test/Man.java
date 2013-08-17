package org.test;
import com.gigaspaces.annotation.pojo.SpaceClass;

@SpaceClass
public class Man {
	public Man(){
		
	}
	
	@Override
	public boolean equals(Object ob){
		return ob.getClass() == Man.class;
	}
}