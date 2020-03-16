package com.waniuzhang.hive.security;

import com.google.common.base.Joiner;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Created by 1 on 2020/3/11.
 */
public class HiveAdmin extends AbstractSemanticAnalyzerHook {

    private static String[] admins = {"hadoop"};

    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast) throws SemanticException {
        switch (ast.getToken().getType()){
            case HiveParser.TOK_CREATEDATABASE:
            case HiveParser.TOK_DROPDATABASE:
            case HiveParser.TOK_CREATEROLE:
            case HiveParser.TOK_DROPROLE:
            case HiveParser.TOK_GRANT:
            case HiveParser.TOK_REVOKE:
            case HiveParser.TOK_GRANT_ROLE:
            case HiveParser.TOK_REVOKE_ROLE:
            case HiveParser.TOK_CREATETABLE:

                String userName = null;
                if(SessionState.get() != null && SessionState.get().getAuthenticator().getUserName() != null){
                     userName = SessionState.get().getAuthenticator().getUserName();
                }

                boolean isAdmin = false;
                for (String admin: admins) {
                    //判断是否是admin
                    if(admin.equalsIgnoreCase(userName)){
                        isAdmin = true;
                        break;
                    }
                }

                if(!isAdmin){
                    throw  new SemanticException(userName + "is not admin ,except "+ Joiner.on(",").join(admins));
                }
                break;
            default:
                break;

        }
        return ast;
    }
}
