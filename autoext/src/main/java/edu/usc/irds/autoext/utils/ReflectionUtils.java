package edu.usc.irds.autoext.utils;

import edu.usc.irds.autoext.base.EditDistanceComputer;
import edu.usc.irds.autoext.tree.TreeNode;

/**
 * Created by tg on 2/29/16.
 */
public class ReflectionUtils {

    /**
     * this method instantiates a class
     * @param clsName name of class
     * @return instance
     *
     */
    public static <T> T instantiate(String clsName) {
        try {
            Class<?> aClass = Class.forName(clsName, true, ReflectionUtils.class.getClassLoader());
            Object instance = aClass.newInstance();
            return (T) instance;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * this method instantiates an instance of edit distance computer
     * @param clsName name of class
     * @return
     */
    public static EditDistanceComputer<TreeNode> intantiateEDComputer(String clsName){
        return instantiate(clsName);
    }
}
