package com.profitera.dc.tools;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JTextPane;
import javax.swing.SwingUtilities;
import javax.swing.event.CaretEvent;
import javax.swing.event.CaretListener;
import javax.swing.text.BadLocationException;
import javax.swing.text.Element;
import javax.swing.text.Style;
import javax.swing.text.StyleConstants;
import javax.swing.text.StyleContext;
import javax.swing.text.StyledDocument;

import com.profitera.dc.tools.impl.ISpanOptionProvider;
import com.profitera.math.MathUtil;
import com.profitera.swing.ColorUtil;
import com.profitera.swing.menu.AbstractPopupMenuMouseListener;
import com.profitera.swing.scroll.JScrollPaneUtil;

public class TextFilePreviewer {
  
  private static final String CUR_STYLE = "current";
  private static final String REG_STYLE = "regular";
  private static final String LINE_STYLE = "line";
  
  public static class Span {
    private final int start;
    private final int end;

    public Span(int start, int end) {
      this.start = start;
      this.end = end;
    }
    public int getStart() {
      return start;
    }
    public int getEnd() {
      return end;
    }    
    public int getLength() {
      return end == 0 ? 0 : end - start + 1;
    }
    public String toString() {
      return "{" + start + "-" + end + "}";
    }
  }

  private JTextPane textPane;
  private StyledDocument doc;
  private File targetFile;
  private JComponent component;
  private Span currentSpan = null;
  private ISpanOptionProvider spanOptions;
  private String currentDelimiter;

  public TextFilePreviewer() {
    
  }
  
  private JTextPane getTextPane() {
    if (textPane == null) {
      textPane = new JTextPane();
      //textPane.setEditorKit(new WrapEditorKit());
      textPane.setEditable(false);
      textPane.getCaret().setVisible(true);
      doc = textPane.getStyledDocument();
      StyleContext context = StyleContext.getDefaultStyleContext();
      Style def = context.getStyle(StyleContext.DEFAULT_STYLE);
      // Regular
      Style regular = getDocument().addStyle(REG_STYLE, def);
      
      StyleConstants.setFontFamily(regular, Font.MONOSPACED);
      StyleConstants.setFontSize(regular, 14);
      // Current span
      Style s = getDocument().addStyle(CUR_STYLE, regular);
      StyleConstants.setBold(s, true);
      StyleConstants.setForeground(s, Color.BLUE);
      StyleConstants.setUnderline(s, true);
      // Selected Line
      Style line = getDocument().addStyle(LINE_STYLE, def);
      StyleConstants.setBackground(line, ColorUtil.decodeARGB("DCDCD3"));
      //
      textPane.addCaretListener(new CaretListener() {
        int oldDot = 0;
        public void caretUpdate(final CaretEvent e) {
          final Element oldPara = getDocument().getParagraphElement(oldDot);
          final Element newPara = getDocument().getParagraphElement(e.getDot());
          SwingUtilities.invokeLater(new Runnable() {
            public void run() {
              if (oldPara != newPara) {
                styleLine(oldDot);
                styleLine(e.getDot());
              }
              oldDot = e.getDot();
            }
          });
        }
      });
      textPane.addMouseListener(new AbstractPopupMenuMouseListener(){
        @Override
        protected JPopupMenu getMenu() {
          return spanOptions != null ? spanOptions.getSpanOptions(getSelectionRange()): null;
        }});
      
    }
    return textPane;
  }
  
  public Span getSelectionRange() {
    int selectionStart = getTextPane().getSelectionStart();
    int selectionEnd = getTextPane().getSelectionEnd();
    Element startP = getDocument().getParagraphElement(selectionStart);
    Element endP = getDocument().getParagraphElement(selectionEnd);
    if (startP != endP) {
      return null;
    } else {
      int start = Math.min(selectionStart, selectionEnd) - startP.getStartOffset();
      int end = Math.max(selectionStart, selectionEnd) - startP.getStartOffset();
      return new Span(start + 1, end);
    }
  }
  
  private StyledDocument getDocument() {
    if (doc == null) {
      getTextPane();
    }
    return doc;
  }
  public void setFile(File target, String encoding) {
    this.targetFile = target;
    clear();
    int rows = 100;
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(targetFile), encoding));
      for(int i = 0; i < rows; i++) {
        String line = br.readLine();
        if (line == null) {
          break;
        }
        DocumentLine d = new DocumentLine(getDocument().getLength(), line);
        getDocument().insertString(getDocument().getLength(), line + "\n", null);
        stylePara(d);
      }
      br.close();
      getTextPane().setCaretPosition(0);
    } catch (BadLocationException e) {
      // This should be impossible
      e.printStackTrace();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

	public void clear() {
		try{
			getDocument().remove(0, getDocument().getLength());
		}catch(BadLocationException e){
		// This should be impossible
      e.printStackTrace();
		}
	}
  private Span getAdjustedSpan(DocumentLine currentLine, Span span, String delimiter) {
    if (delimiter == null) {
      return span;
    }
    try {
      String text = currentLine.text;
      int lastEnd = 0;
      int lastLastEnd = 0;
      for (int field = 0; lastEnd != -1 && field < span.getStart(); field++) {
        lastLastEnd = lastEnd;
        lastEnd = text.indexOf(delimiter, lastEnd + 1);
      }
      return new Span(lastLastEnd+2, lastEnd);
    } catch (Exception e) {
      System.out.println(e.getMessage());
      return null;
    }
  }
  private class DocumentLine {
    final String text;
    final int startIndex;
    public DocumentLine(int i, String t) {
      text = t;
      startIndex = i;
    }
    public int getStartOffset() {
      return startIndex;
    }
    public int getEndOffset() {
      return startIndex + text.length();
    }
    public boolean isInside(int caretPosition) {
      return caretPosition >= getStartOffset() && caretPosition <= getEndOffset();
    }
    public String toString() {
      return "Line at " + getStartOffset() + "-" + getEndOffset() + " " + text;
    }
  }
  void setSpan(final Span newSpan, String delimiter) {
    currentSpan = newSpan;
    currentDelimiter = delimiter;
    int caretPosition = getTextPane().getCaretPosition();
    getDocument().setCharacterAttributes(0, getDocument().getLength(), getDocument().getStyle(REG_STYLE), true);
    DocumentLine currentLine = getDocumentLine(0, getDocument());
    while (currentLine != null) {
      stylePara(currentLine);
      // If the caret is on this line we should reposition it, provided the 
      // line is long enough to get there, this does not properly handle the case where
      // the line is long enough for the start, but too long for the end
      if(newSpan == null) {
        return;
      }
      boolean isCaretOnLine = MathUtil.between(caretPosition, currentLine.getStartOffset(), currentLine.getEndOffset());
      if (isCaretOnLine) {
        final Span s = getAdjustedSpan(currentLine, newSpan, delimiter);
        if (s == null) {
          break;
        }
        if (isCaretOnLine && s.getEnd() + currentLine.getStartOffset() + 1 <= currentLine.getEndOffset()) {
          getTextPane().setCaretPosition(currentLine.getStartOffset() + s.getEnd());
          final int startOffset = currentLine.getStartOffset() + s.getStart() - 1;
          if (getTextPane().getText()!=null && startOffset >= 0) {
            SwingUtilities.invokeLater(new Runnable(){
              public void run() {
              	try{
              		getTextPane().setCaretPosition(startOffset);
              		// There seems to be a bug in JtextPane that keeps turning my Caret invisible
              		getTextPane().getCaret().setVisible(true);
              	}catch(IllegalArgumentException e){
              		// ignore
              	}
              }
            });
          }
        }
      }
      // +1 here for the newline that was excluded
      currentLine = getDocumentLine(currentLine.getEndOffset() + 1, getDocument());
    }
  }
  
  private DocumentLine getDocumentLine(int startIndex, StyledDocument document) {
    int len = document.getLength();
    if (startIndex >= len) {
      return null;
    }
    String fullText;
    try {
      fullText = document.getText(0, len);
    } catch (BadLocationException e) {
      throw new IllegalArgumentException(e);
    }
    int indexOf = fullText.indexOf("\n", startIndex);
    if (indexOf == -1) {
      indexOf = fullText.length();
    }
    return new DocumentLine(startIndex, fullText.substring(startIndex, indexOf));
  }

  private void styleLine(int position) {
    //TODO: fix this
  }

  private void stylePara(final DocumentLine selectedPara) {
    int start = selectedPara.getStartOffset();
    int len = selectedPara.getEndOffset() - selectedPara.getStartOffset();
    // reset style to regular
    getDocument().setCharacterAttributes(start, len, getDocument().getStyle(REG_STYLE), true);
    // Set bold on specified location
    Span adjustedSpan = getAdjustedSpan(selectedPara, getSpan(), getDelimiter());
    if (adjustedSpan != null && adjustedSpan.getLength() > 0) {
      int startOffset = start + adjustedSpan.start - 1;
      int spanLength = adjustedSpan.getLength();
      getDocument().setCharacterAttributes(startOffset, spanLength, getDocument().getStyle(CUR_STYLE), false);
    }
  	// highlight the selected line
    int caretPosition = getTextPane().getCaretPosition();
    if (selectedPara.isInside(caretPosition)){
      getDocument().setCharacterAttributes(start, len, getDocument().getStyle(LINE_STYLE), false);
    } 
  }
  
  private Span getSpan() {
    return currentSpan;
  }
  private String getDelimiter() {
    return currentDelimiter;
  }

  public JComponent getComponent() {
    if (component == null) {
      JPanel p = new JPanel(new BorderLayout());
      p.add(getTextPane());
      component = JScrollPaneUtil.wrap(p);
    }
    return component;
  }

  public void setSpanOptions(ISpanOptionProvider spanOptions) {
    this.spanOptions = spanOptions;
  }

  public String getText(){
  	if(targetFile==null) {
  	  return null;
  	}
  	return getTextPane().getText();
  }

  public File getFile() {
    return targetFile;
  }
}
