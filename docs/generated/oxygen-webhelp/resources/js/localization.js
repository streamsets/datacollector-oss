/*

Oxygen Webhelp plugin
Copyright (c) 1998-2014 Syncro Soft SRL, Romania.  All rights reserved.
Licensed under the terms stated in the license file EULA_Webhelp.txt 
available in the base directory of this Oxygen Webhelp plugin.

*/

/**
 * get translated messages
 */
function getLocalization(localizationKey) {
  var toReturn=localizationKey;
  if((localizationKey in localization)){
    toReturn=localization[localizationKey];
  }
  debug('getLocalization('+localizationKey+')='+toReturn);
  return toReturn;
}
