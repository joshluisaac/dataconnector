package com.profitera.dc.tools.impl;

import javax.swing.text.MutableAttributeSet;
import javax.swing.text.StyledEditorKit;
import javax.swing.text.ViewFactory;



public class WrapEditorKit extends StyledEditorKit {
    private static final long serialVersionUID = 1L;
    public static final String LINE_BREAK_ATTRIBUTE_NAME = "line_break_attrib";
    ViewFactory defaultFactory=new WrapColumnFactory();
    public ViewFactory getViewFactory() {
        return defaultFactory;
    }

    public MutableAttributeSet getInputAttributes() {
        MutableAttributeSet mAttrs=super.getInputAttributes();
        mAttrs.removeAttribute(LINE_BREAK_ATTRIBUTE_NAME);
        return mAttrs;
    }
}