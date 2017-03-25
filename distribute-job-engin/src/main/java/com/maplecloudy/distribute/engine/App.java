package com.maplecloudy.distribute.engine;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;


@RequestScoped
public class App {
  final AppContext context;

  @Inject
  App(AppContext ctx) {
    context = ctx;
  }
}
