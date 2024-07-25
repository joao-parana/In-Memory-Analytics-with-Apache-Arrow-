/**
 * O programa em questão é escrito em C++ e tem como objetivo ler um arquivo CSV 
 * e exibir os dados na saída do terminal.
 */
// MIT License
//
// Copyright (c) 2021 Packt
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include <arrow/csv/api.h>  // the csv functions and objects
#include <arrow/io/api.h>   // for opening the file
#include <arrow/table.h>    // to read the data into a table
#include <iostream>         // to output to the terminal


int main(int argc, char** argv) {
  /*
  Tenta abrir o arquivo CSV chamado "train.csv". O resultado é armazenado em 
  um objeto maybe_input, que pode conter um valor válido ou um erro. 
  */
  auto maybe_input =
      arrow::io::ReadableFile::Open("../../sample_data/train.csv");

  /*
  É verificado se a abertura do arquivo foi bem-sucedida usando o método ok() do 
  objeto maybe_input. Se não for, uma mensagem de erro é exibida no terminal usando
  std::cerr e o programa é encerrado com um código de retorno 1.
  */   
  if (!maybe_input.ok()) {
    // handle any file open errors
    std::cerr << maybe_input.status().message() << std::endl;
    return 1;
  }

  /*
  Se a abertura do arquivo for bem-sucedida, o ponteiro compartilhado input é inicializado
  com o valor contido em maybe_input.
  */
  std::shared_ptr<arrow::io::InputStream> input = *maybe_input;

  /*
  são criados objetos para configurar as opções de leitura do arquivo CSV
  */
  auto io_context = arrow::io::default_io_context(); /* obtém o contexto de E/S padrão */
  auto read_options = arrow::csv::ReadOptions::Defaults(); /* obtém as opções de leitura padrão */
  auto parse_options = arrow::csv::ParseOptions::Defaults(); /* obtém as opções de análise padrão */
  auto convert_options = arrow::csv::ConvertOptions::Defaults(); /* obtém as opções de conversão padrão */

  /*
  A função arrow::csv::TableReader::Make é chamada com os objetos de contexto, entrada e opções
  de leitura, análise e conversão. Essa função cria um leitor de tabela para o arquivo CSV fornecido.
  */
  auto maybe_reader = arrow::csv::TableReader::Make(
      io_context, input, read_options, parse_options, convert_options);

  /*
  verificando se a criação do leitor de tabela (maybe_reader) foi bem-sucedida. Se não for, significa
  que ocorreu algum erro durante a instância do leitor. Nesse caso, o código exibe a mensagem de erro
  usando std::cerr e retorna 1 para indicar que houve um erro.
  */
  if (!maybe_reader.ok()) {
    // handle any instantiation errors
    std::cerr << maybe_reader.status().message() << std::endl;
    return 1;
  }
  /*
  Estamos atribuindo o valor de maybe_reader a um ponteiro compartilhado (std::shared_ptr) chamado reader.
  Isso nos permite acessar os métodos e dados do leitor de tabela posteriormente.
  */
  std::shared_ptr<arrow::csv::TableReader> reader = *maybe_reader;
  // finally read the data from the file
  /*
  Aqui estamos lendo os dados do arquivo usando o leitor de tabela. O método Read() retorna um objeto
   Result em maybe_table que contém a tabela lida. Estamos verificando se a leitura foi bem-sucedida usando
   maybe_table.ok(). Se ocorrer algum erro durante a leitura, como erros de sintaxe CSV ou erros de
   conversão de tipo, a mensagem de erro é exibida e o código retorna 1.
  */
  auto maybe_table = reader->Read();
  if (!maybe_table.ok()) {
    // handle any errors such as CSV syntax errors
    // or failed type conversion errors, etc.
    std::cerr << maybe_table.status().message() << std::endl;
    return 1;
  }

  /*
  Aqui estamos atribuindo o valor de maybe_table a um ponteiro compartilhado (std::shared_ptr) chamado table.
  Isso nos permite acessar os dados da tabela posteriormente. Em seguida, estamos imprimindo a representação
  em string da tabela usando table->ToString() e exibindo-a no console usando std::cout.
  */
  std::shared_ptr<arrow::Table> table = *maybe_table;
  std::cout << table->ToString() << std::endl;
}