package com.profitera.dc.tools.impl;

import javax.swing.JPopupMenu;

import com.profitera.dc.tools.TextFilePreviewer.Span;

public interface ISpanOptionProvider {
  public JPopupMenu getSpanOptions(Span s);
}
