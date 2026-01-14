-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS demos;
USE CATALOG demos;
CREATE SCHEMA IF NOT EXISTS dsa;
USE dsa;
CREATE VOLUME IF NOT EXISTS demos.dsa.raw;
