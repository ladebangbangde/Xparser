package com.ldbbd.xparser.interfaces;

public interface ParserFactory extends Factory {
    /** Creates a new parser. */
    Parser create(Context context);

    /** Context provided when a parser is created. */
    interface Context {
//        CatalogManager getCatalogManager();
//
//        PlannerContext getPlannerContext();
    }

    /** Default implementation for {@link Context}. */
    class DefaultParserContext implements Context {
//        private final CatalogManager catalogManager;
//        private final PlannerContext plannerContext;
//
//        public DefaultParserContext(CatalogManager catalogManager, PlannerContext plannerContext) {
//            this.catalogManager = catalogManager;
//            this.plannerContext = plannerContext;
//        }
//
//        @Override
//        public CatalogManager getCatalogManager() {
//            return catalogManager;
//        }
//
//        @Override
//        public PlannerContext getPlannerContext() {
//            return plannerContext;
//        }
    }
}
