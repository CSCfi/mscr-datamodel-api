/*
 * Licensed under the European Union Public Licence (EUPL) V.1.1 
 */
package fi.vm.yti.datamodel.api.utils;

final public class ErrorMessage {
    
    final public static String NOTCREATED = toJs("{'errorMessage':'Resource was not created'}");
    final public static String UNAUTHORIZED = toJs("{'errorMessage':'Unauthorized'}");
    final public static String LOCKED = toJs("{'errorMessage':'Locked'}");
    final public static String INVALIDIRI = toJs("{'errorMessage':'Invalid ID'}");
    final public static String INVALIDPREFIX = toJs("{'errorMessage':'Invalid PREFIX'}");
    final public static String USEDIRI = toJs("{'errorMessage':'ID is already in use'}");
    final public static String UNEXPECTED = toJs("{'errorMessage':'Unexpected error'}");
    final public static String NOTFOUND = toJs("{'errorMessage':'Not found'}");
    final public static String LANGNOTDEFINED = toJs("{'errorMessage':'Not defined in given language'}");
    final public static String NOTREMOVED = toJs("{'errorMessage':'Not removed'}");
    final public static String STATUS = toJs("{'errorMessage':'Resource status restricts removing'}");
    final public static String DEPEDENCIES = toJs("{'errorMessage':'Resource dependencies restricts removing'}");
    final public static String INVALIDVOCABULARY = toJs("{'errorMessage':'Invalid SKOSMOS ID'}");
    final public static String INVALIDPARAMETER = toJs("{'errorMessage':'Invalid API parameters'}");
    final public static String NOTACCEPTED = toJs("{'errorMessage':'Not accepted'}");
    
    private static String toJs(String jsonString) {
        return jsonString.replaceAll("'", "\"");
    } 
    
}
