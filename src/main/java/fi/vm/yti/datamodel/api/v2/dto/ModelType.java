package fi.vm.yti.datamodel.api.v2.dto;

public enum ModelType {
    /**
     * FI: Soveltamisprofiili,
     * EN: Application profile
     */
    PROFILE,
    /**
     * Entinen: Tietokomponenttikirjasto.
     * FI: Ydintietomalli,
     * EN: Core vocabulary
     */
    LIBRARY,
    SCHEMA,
    CROSSWALK
}
